package com.github.maqroll;

import com.github.maqroll.deserializers.ReplicationEventPayloadDeserializer;
import com.github.maqroll.deserializers.RotateEventDeserializer;
import com.github.maqroll.deserializers.TableMapEventDeserializer;
import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ReplicationEventHeader;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.ReplicationEventType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelDeserializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelDeserializer.class);
  private final ExecutorService executorService;
  private final BlockingQueue<Future<ReplicationEvent>> tasks = new LinkedBlockingQueue<>();
  private final ChannelHandlerContext ctx;
  private final Channel ch;
  private final Map<ReplicationEventType, ReplicationEventPayloadDeserializer<?>> deserializers =
      new HashMap<>();
  private final Lock lock = new ReentrantLock();
  private final Condition empty;

  // Results and exceptions get notified through ctx.
  public ParallelDeserializer(int capacity, final ChannelHandlerContext ctx) {
    this.ctx = ctx;
    this.ch = ctx.channel();

    empty = lock.newCondition();

    deserializers.put(ReplicationEventType.ROTATE_EVENT, new RotateEventDeserializer());
    deserializers.put(ReplicationEventType.TABLE_MAP_EVENT, new TableMapEventDeserializer());

    if (capacity < 1) {
      throw new IllegalArgumentException(
          String.format(
              "ParallelDeserializer capacity should be greater than 1. Invalid value: %d",
              capacity));
    }

    executorService =
        Executors.newFixedThreadPool(
            capacity + 1 /* one for the teapot */,
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true); // prevent to keep JVM hanging
                return t;
              }
            });

    executorService.submit(
        new Runnable() {
          @Override
          public void run() {
            Future<ReplicationEvent> fEvent;
            while (true) {
              try {
                fEvent = tasks.poll(1, TimeUnit.SECONDS);

                if (fEvent != null) {
                  processResult(fEvent);
                }

                try {
                  lock.lock();
                  if (!pending()) {
                    empty.signal();
                  }
                } finally {
                  lock.unlock();
                }
              } catch (InterruptedException e) {
                // TODO Improve message
                ctx.fireExceptionCaught(e);
              }
            }
          }

          void processResult(Future<ReplicationEvent> fEvent) {
            try {
              final ReplicationEvent evt = fEvent.get();
              LOGGER.info("Notifying replication event {}", evt);
              ctx.fireChannelRead(evt);
            } catch (InterruptedException e) {
              // TODO improve message
              ctx.fireExceptionCaught(e);
            } catch (ExecutionException e) {
              // TODO improve message
              ctx.fireExceptionCaught(e.getCause());
            }
          }
        });
  }

  public Future<ReplicationEvent> getTask() {
    return tasks.poll();
  }

  public boolean pending() {
    return !tasks.isEmpty();
  }

  public void addPacket(
      final ReplicationEventHeader header,
      final ByteBuf buf,
      final ChecksumType checksumType,
      Channel ch) {
    final Future<ReplicationEvent> submit =
        executorService.submit(
            new Callable<ReplicationEvent>() {
              @Override
              public ReplicationEvent call() throws Exception {
                try {
                  final ReplicationEventPayloadDeserializer<?> deserializer =
                      deserializers.get(header.getEventType());
                  ReplicationEventPayload payload = deserializer.deserialize(buf, ch);
                  return new ReplicationEvent() {
                    @Override
                    public ReplicationEventHeader header() {
                      return header;
                    }

                    @Override
                    public ReplicationEventPayload payload() {
                      return payload;
                    }
                  };
                } finally {
                  buf.release();
                }
              }
            });
    tasks.add(submit);

    // For certain tasks we are going to wait before returning because next
    // packets can't be deserialized until it finishes
    if (ReplicationEventType.TABLE_MAP_EVENT.equals(header.getEventType())) {
      try {
        lock.lock();
        while (pending()) {
          empty.await();
        }
      } catch (InterruptedException e) {
        // TODO
      } finally {
        lock.unlock();
      }
      waitUntilFinishesAndUpdateCurrentTableMap(submit, ch);
      // Important!!!
      // At this point all previous tasks have finished
      // so it's safe to update current table map
    }
  }

  private void waitUntilFinishesAndUpdateCurrentTableMap(Future<ReplicationEvent> evt, Channel ch) {
    try {
      TableMapEventPayload tableMap = (TableMapEventPayload) evt.get().payload();
      tableMap.setCurrent(ch);
    } catch (InterruptedException e) {
      // ignore
    } catch (ExecutionException e) {
      // ignore
    }
  }
}

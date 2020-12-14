package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ReplicationEventHeader;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.ReplicationEventType;
import com.github.mheath.netty.codec.mysql.RotateEventPayload;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ParallelDeserializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelDeserializer.class);
  private final ExecutorService executorService;
  private final BlockingQueue<Future<ReplicationEvent>> tasks = new LinkedBlockingQueue<>();
  private final ChannelHandlerContext ctx;
  private final ReplicationEventPayloadDeserializer<RotateEventPayload> deserializer =
      new RotateEventDeserializer();

  // Results and exceptions get notified through ctx.
  public ParallelDeserializer(int capacity, final ChannelHandlerContext ctx) {
    this.ctx = ctx;

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
      final ReplicationEventHeader header, final ByteBuf buf, final ChecksumType checksumType) {
    if (ReplicationEventType.ROTATE_EVENT.equals(header.getEventType())) {
      final Future<ReplicationEvent> submit =
          executorService.submit(
              new Callable<ReplicationEvent>() {
                @Override
                public ReplicationEvent call() throws Exception {
                  try {
                    if (ReplicationEventType.ROTATE_EVENT.equals(header.getEventType())) {
                      final RotateEventPayload payload =
                          deserializer.deserialize(buf, Charset.defaultCharset());
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
                    } else {
                      return null;
                    }
                  } finally {
                    buf.release();
                  }
                }
              });

      // For certain tasks we are going to wait before returning because next
      // packets can't be deserialized until it finishes
 /*   if (ReplicationEventType.ANNOTATE_ROWS_EVENT.equals(header.getEventType())) {
      waitUntilFinishes(submit);
      return; // make sure has finished before returning from here
    } else {*/
      tasks.add(submit);
      /* } */
    }
  }

  private void waitUntilFinishes(Future<ReplicationEvent> evt) {
    try {
      evt.get();
    } catch (InterruptedException e) {
      // ignore
    } catch (ExecutionException e) {
      // ignore
    }
  }
}

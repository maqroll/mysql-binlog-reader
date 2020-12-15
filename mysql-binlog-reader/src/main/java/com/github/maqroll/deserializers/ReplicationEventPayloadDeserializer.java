package com.github.maqroll.deserializers;

import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

public interface ReplicationEventPayloadDeserializer<T extends ReplicationEventPayload> {

  T deserialize(ByteBuf buf, Charset charset);
}

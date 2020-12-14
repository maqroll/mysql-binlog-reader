package com.github.maqroll;

import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

public interface ReplicationEventPayloadDeserializer<T> {

  T deserialize(ByteBuf buf, Charset charset);
}

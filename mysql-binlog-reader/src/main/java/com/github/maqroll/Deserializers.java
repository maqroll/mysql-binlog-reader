package com.github.maqroll;

import com.github.maqroll.deserializers.DeleteRowsEventDeserializer;
import com.github.maqroll.deserializers.ReplicationEventPayloadDeserializer;
import com.github.maqroll.deserializers.RotateEventDeserializer;
import com.github.maqroll.deserializers.TableMapEventDeserializer;
import com.github.maqroll.deserializers.UpdateRowsEventDeserializer;
import com.github.maqroll.deserializers.WriteRowsEventDeserializer;
import com.github.mheath.netty.codec.mysql.ReplicationEventType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Deserializers {
  private static final Map<ReplicationEventType, ReplicationEventPayloadDeserializer<?>>
      deserializers = new HashMap<>();

  private Deserializers() {}

  static {
    deserializers.put(ReplicationEventType.ROTATE_EVENT, new RotateEventDeserializer());
    deserializers.put(ReplicationEventType.TABLE_MAP_EVENT, new TableMapEventDeserializer());
    deserializers.put(ReplicationEventType.WRITE_ROWS_EVENTv1, new WriteRowsEventDeserializer());
    deserializers.put(ReplicationEventType.DELETE_ROWS_EVENTv1, new DeleteRowsEventDeserializer());
    deserializers.put(ReplicationEventType.UPDATE_ROWS_EVENTv1, new UpdateRowsEventDeserializer());
  }

  public static ReplicationEventPayloadDeserializer<?> get(ReplicationEventType type) {
    Objects.requireNonNull(type, "Deserializers.get requires a non-null argument");
    ReplicationEventPayloadDeserializer<?> deserializer = deserializers.get(type);

    if (deserializer == null) {
      throw new IllegalStateException("Missing deserializer for event of type " + type.toString());
    }

    return deserializer;
  }
}

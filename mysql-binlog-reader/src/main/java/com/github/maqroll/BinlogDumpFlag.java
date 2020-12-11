package com.github.maqroll;

public enum BinlogDumpFlag {
  // if there is no more event to send
  // send an EOF_Packet instead of blocking the connection
  BINLOG_DUMP_NON_BLOCK,
  BINLOG_SEND_ANNOTATE_ROWS_EVENT /* TODO: just mariadb?? */
}

package com.github.maqroll;

public enum BinlogDumpFlag {
  //if there is no more event to send
  // send an EOF_Packet instead of blocking the connection
  BINLOG_DUMP_NON_BLOCK
}

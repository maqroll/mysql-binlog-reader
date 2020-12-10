package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.AbstractPacketEncoder;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.MysqlCharacterSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

public class BinlogDumpEncoder extends AbstractPacketEncoder<BinlogDumpCommand> {

    public BinlogDumpEncoder() {}

    @Override
    protected void encodePacket(ChannelHandlerContext ctx, BinlogDumpCommand packet, ByteBuf buf) {
      final Charset serverCharset =
          MysqlCharacterSet.getServerCharsetAttr(ctx.channel()).getCharset();

      buf.writeIntLE(packet.getPos());
      buf.writeShortLE((int) CodecUtils.toLong(packet.getFlags()));
      buf.writeIntLE(packet.getServerId());
      CodecUtils.writeLengthEncodedString(buf, packet.getFileName(), serverCharset);
    }
}
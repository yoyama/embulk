package org.embulk.spi.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.google.common.base.Throwables;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.value.Value;
import org.jruby.Ruby;
import org.jruby.RubyString;
import org.jruby.util.ByteList;
import org.jcodings.specific.ASCIIEncoding;

public class RubyValueApi
{
    public static Value fromMessagePack(RubyString content)
    {
        ByteList list = content.getByteList();
        try {
            return MessagePack.newDefaultUnpacker(list.unsafeBytes(), list.begin(), list.length()).unpackValue();
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private static class OpenByteArrayOutputStream
            extends ByteArrayOutputStream
    {
        public byte[] getBuffer()
        {
            return buf;
        }

        public int getCount()
        {
            return count;
        }
    }

    public static RubyString toMessagePack(Ruby runtime, Value value)
    {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packValue(value);
            packer.flush();  // this will be unnecessary once #329 is merged: https://github.com/msgpack/msgpack-java/pull/329
            MessageBuffer mb = packer.toMessageBuffer();
            ByteList list = new ByteList(mb.array(), mb.arrayOffset(), mb.size(), ASCIIEncoding.INSTANCE, false);
            return RubyString.newString(runtime, list);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }
}

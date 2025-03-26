package org.mv.os.fredis.source;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RedisStreamSplitSerializer implements SimpleVersionedSerializer<RedisStreamSplit> {

    @Override
    public int getVersion() {
        return 1;
    }
    @Override
    public byte[] serialize(RedisStreamSplit split) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(split.getStreamKey());
            out.writeUTF(split.getOffset());
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public RedisStreamSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bis)) {
            return new RedisStreamSplit(in.readUTF(), in.readUTF());
        }
    }
}

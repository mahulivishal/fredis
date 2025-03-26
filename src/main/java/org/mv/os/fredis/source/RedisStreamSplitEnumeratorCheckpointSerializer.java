package org.mv.os.fredis.source;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RedisStreamSplitEnumeratorCheckpointSerializer implements SimpleVersionedSerializer<List<RedisStreamSplit>> {
    private final RedisStreamSplitSerializer splitSerializer = new RedisStreamSplitSerializer();
    @Override
    public int getVersion() {
        return 1;
    }
    @Override
    public byte[] serialize(List<RedisStreamSplit> splits){
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(splits.size());
            for (RedisStreamSplit split : splits) {
                byte[] serialized = splitSerializer.serialize(split);
                out.writeInt(serialized.length);
                out.write(serialized);
            }
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public List<RedisStreamSplit> deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bis)) {
            int size = in.readInt();
            List<RedisStreamSplit> splits = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] data = new byte[len];
                in.readFully(data);
                splits.add(splitSerializer.deserialize(version, data));
            }
            return splits;
        }
    }
}

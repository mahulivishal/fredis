package org.mv.os.fredis.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;

public class RedisStreamSource implements Source<String, RedisStreamSplit, List<RedisStreamSplit>> {
    private final List<String> streamKeys;

    public RedisStreamSource(List<String> streamKeys) {
        this.streamKeys = streamKeys;
    }
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }
    @Override
    public SourceReader<String, RedisStreamSplit> createReader(SourceReaderContext readerContext) {
        return new RedisStreamSourceReader(readerContext);
    }
    @Override
    public SplitEnumerator<RedisStreamSplit, List<RedisStreamSplit>> createEnumerator(SplitEnumeratorContext<RedisStreamSplit> enumContext) {
        return new RedisStreamSplitEnumerator(enumContext, streamKeys);
    }
    @Override
    public SplitEnumerator<RedisStreamSplit, List<RedisStreamSplit>> restoreEnumerator(SplitEnumeratorContext<RedisStreamSplit> context, List<RedisStreamSplit> checkpoint) {
        return new RedisStreamSplitEnumerator(context, streamKeys);
    }
    @Override
    public SimpleVersionedSerializer<RedisStreamSplit> getSplitSerializer() {
        return new RedisStreamSplitSerializer();
    }
    @Override
    public SimpleVersionedSerializer<List<RedisStreamSplit>> getEnumeratorCheckpointSerializer() {
        return new RedisStreamSplitEnumeratorCheckpointSerializer();
    }
}











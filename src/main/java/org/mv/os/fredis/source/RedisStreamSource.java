package org.mv.os.fredis.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.mv.os.fredis.config.Configs;

import java.util.List;

public class RedisStreamSource implements Source<String, RedisStreamSplit, List<RedisStreamSplit>> {
    private final List<String> streamKeys;
    private final Configs redisConfigs;

    public RedisStreamSource(List<String> streamKeys, Configs configs) {
        this.streamKeys = streamKeys;
        this.redisConfigs = configs;
    }
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }
    @Override
    public SourceReader<String, RedisStreamSplit> createReader(SourceReaderContext readerContext) {
        return new RedisStreamSourceReader(readerContext, redisConfigs);
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











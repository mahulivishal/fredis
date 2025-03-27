package org.mv.os.fredis.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.mv.os.fredis.config.InternalConfigs;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisStreamSplitEnumerator implements SplitEnumerator<RedisStreamSplit, List<RedisStreamSplit>> {

    private final SplitEnumeratorContext<RedisStreamSplit> context;
    private final List<String> streamKeys; // configured Redis stream keys
    private final Map<Integer, RedisStreamSplit> assignedSplits = new HashMap<>();

    public RedisStreamSplitEnumerator(SplitEnumeratorContext<RedisStreamSplit> context, List<String> streamKeys) {
        this.context = context;
        this.streamKeys = streamKeys;
    }

    @Override
    public void start() {
        // Assign one split per reader
        int i = 0;
        for (String streamKey : streamKeys) {
            context.assignSplit(new RedisStreamSplit(streamKey, InternalConfigs.initialSplitOffset), i % context.currentParallelism());
            i++;
        }
    }
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // No-op: we assign eagerly in start()
    }
    @Override
    public void addSplitsBack(List<RedisStreamSplit> splits, int subtaskId) {
        for (RedisStreamSplit split : splits) {
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void addReader(int i) { }

    @Override
    public List<RedisStreamSplit> snapshotState(long checkpointId) {
        return streamKeys.stream()
                .map(key -> new RedisStreamSplit(key, InternalConfigs.initialSplitOffset))
                .collect(Collectors.toList());
    }
    @Override
    public void close() {}
}

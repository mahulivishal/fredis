package org.mv.os.fredis.source;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.mv.os.fredis.config.Configs;
import org.mv.os.fredis.config.Constants;
import org.mv.os.fredis.config.InternalConfigs;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RedisStreamSourceReader implements SourceReader<String, RedisStreamSplit> {

    private final SourceReaderContext context;
    private final Queue<RedisStreamSplit> assignedSplits = new ArrayDeque<>();
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;

    public RedisStreamSourceReader(SourceReaderContext context, Configs configs) {
        this.context = context;
        RedisURI redisUri = RedisURI.builder().withHost(configs.getRedisUrl()).withPort(configs.getRedisPort())
                .withAuthentication(configs.getRedisUsername(), configs.getRedisPassword().toCharArray()).withSsl(true)
                .withTimeout(Duration.ofSeconds(configs.getConnectionTimeoutInSec())).build();
        this.redisClient = RedisClient.create(redisUri);
        this.connection = redisClient.connect();
        this.commands = connection.sync();
    }
    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        RedisStreamSplit split = assignedSplits.peek();
        if (split == null) {
            Thread.sleep(InternalConfigs.sourceReaderPollIntervalInMS); // avoid tight loop
            return InputStatus.NOTHING_AVAILABLE;
        }
        String stream = split.getStreamKey();
        String offset = split.getOffset();
        // Read 10 messages max from the stream
        List<StreamMessage<String, String>> messages = commands.xread(
                XReadArgs.Builder.count(InternalConfigs.readBufferMaxSize).block(Duration.ofMillis(InternalConfigs.sourceReaderPollIntervalInMS)),
                XReadArgs.StreamOffset.from(stream, offset)
        );
        if (messages == null || messages.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        for (StreamMessage<String, String> msg : messages) {
            String id = msg.getId();
            String data = msg.getBody().toString(); // serialize as needed
            output.collect(data);
            // Update the offset in the split (normally you'd snapshot this)
            assignedSplits.poll();
            assignedSplits.add(new RedisStreamSplit(stream, id));
        }
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<RedisStreamSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(assignedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<RedisStreamSplit> splits) {
        assignedSplits.addAll(splits);
    }
    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() {
        connection.close();
        redisClient.shutdown();
    }
}

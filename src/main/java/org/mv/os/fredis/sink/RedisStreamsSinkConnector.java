package org.mv.os.fredis.sink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.mv.os.fredis.config.Configs;
import org.mv.os.fredis.config.Constants;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class RedisStreamsSinkConnector extends ProcessFunction<Map<String, Object>, String> {
    private transient GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool;
    private final List<Map<String, Object>> buffer = new ArrayList<>();
    private Configs configs;

    public RedisStreamsSinkConnector(Configs configs) {
        this.configs = configs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Configure the connection pool
        if (!configs.getRedisMode().equals(Constants.REDIS_CLUSTER_MODE))
            redisConnectionPool = getRedisConnectionPool();
        else {
            log.error("Redis Streams are currently supported only in Standalone Mode and Synchronous Writes.");
            redisConnectionPool = null;
        }
    }

    private GenericObjectPool<StatefulRedisConnection<String, String>> getRedisConnectionPool() {
        log.info("Connecting to Standalone Redis at: {}", configs.getRedisUrl());
        try {
            GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(configs.getRedisPoolMaxcConnections()); // Maximum number of connections in the pool
            poolConfig.setMaxIdle(configs.getRedisPoolMaxIdle());   // Maximum number of idle connections
            poolConfig.setMinIdle(configs.getRedisPoolMinIdle());
            RedisURI redisUri = RedisURI.builder().withHost(configs.getRedisUrl()).withPort(configs.getRedisPort())
                    .withAuthentication(configs.getRedisUsername(), configs.getRedisPassword().toCharArray()).withSsl(true)
                    .withTimeout(Duration.ofSeconds(30)).build();
            RedisClient redisClient = RedisClient.create(redisUri);
            return ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig);
        } catch (Exception e) {
            log.error("Exception while creating redis standalone client", e);
            return null;
        }
    }

    @Override
    public void processElement(Map<String, Object> record, ProcessFunction<Map<String, Object>, String>.Context context, Collector<String> collector) throws Exception {
        if(null != redisConnectionPool){
            if (!record.containsKey("_key")) return;
            log.debug("record: {}", record);
            buffer.add(record);
            if (buffer.size() >= configs.getBatchSize()) {
                flushToRedisStreams();
            }
        }
    }

    private void flushToRedisStreams() throws Exception {
        if (buffer.isEmpty()) return;
        // Borrowing a Redis connection from the pool
        try (StatefulRedisConnection<String, String> connection = redisConnectionPool.borrowObject()) {
            RedisCommands<String, String> syncCommands = connection.sync();
            List<String> messageIds = new ArrayList<>();
            for (Map<String, Object> record : buffer)
                 messageIds.add(syncCommands.xadd(Constants.REDIS_KEY_SPACE_PREFIX + record.get("_key").toString(), record));
            log.info("REDIS_STREAM_SINK: {}", messageIds);
            buffer.clear();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisConnectionPool != null)
            redisConnectionPool.close();
    }
}

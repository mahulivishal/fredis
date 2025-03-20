package org.mv.os.fredis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class RedisSinkConnector extends ProcessFunction<Map<String, Object>, String> {

    private transient GenericObjectPool<StatefulRedisClusterConnection<String, String>> redisClusterConnectionPool;
    private transient GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool;
    private final List<Map<String, Object>> buffer = new ArrayList<>();
    private static final String REDIS_KEY_SPACE_PREFIX = "vehicle_snapshot_";
    private static final String REDIS_CLUSTER_MODE = "cluster";
    private final int batchSize; // Number of records per bulk write
    private final String redisUrl;
    private final String redisUsername;
    private final String redisPassword;
    private final int redisPort;
    private final int redisPoolMaxcConnections;
    private final int redisPoolMaxIdle;
    private final int redisPoolMinIdle;
    private final String redisMode;

    public RedisSinkConnector(int batchSize, String redisUrl, int redisPort, int redisPoolMaxcConnections, int redisPoolMaxIdle, int redisPoolMinIdle, String redisUsername, String redisPassword, String redisMode) {
        this.batchSize = batchSize;
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
        this.redisPoolMaxcConnections = redisPoolMaxcConnections;
        this.redisPoolMaxIdle = redisPoolMaxIdle;
        this.redisPoolMinIdle = redisPoolMinIdle;
        this.redisUsername = redisUsername;
        this.redisPassword = redisPassword;
        this.redisMode = redisMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Configure the connection pool
        if (redisMode.equals(REDIS_CLUSTER_MODE))
            redisClusterConnectionPool = getRedisClusterConnectionPool();
        else
            redisConnectionPool = getRedisConnectionPool();
        log.info("redisUrl, redisPort, redisUsername, redisPassword, redisMode: {}, {}, {}, {}, {}", redisUrl, redisPort, redisUsername, redisPassword, redisMode);
    }

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> getRedisClusterConnectionPool() {
        log.info("Connecting to Redis Cluster at: {}", redisUrl);
        try {
            GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(redisPoolMaxcConnections); // Maximum number of connections in the pool
            poolConfig.setMaxIdle(redisPoolMaxIdle);   // Maximum number of idle connections
            poolConfig.setMinIdle(redisPoolMinIdle);
            String[] redisNodes = redisUrl.split(",");
            List<RedisURI> redisNodeConnections = new ArrayList<>();
            for (String node : redisNodes) {
                RedisURI redisUri = RedisURI.builder().withHost(node).withPort(redisPort)
                        .withAuthentication(redisUsername, redisPassword.toCharArray()).withSsl(true).build();
                redisNodeConnections.add(redisUri);
            }
            RedisClusterClient redisClient = RedisClusterClient.create(redisNodeConnections);
            return ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig);
        } catch (Exception e) {
            log.error("Exception while creating redis cluster client", e);
            return null;
        }
    }

    private GenericObjectPool<StatefulRedisConnection<String, String>> getRedisConnectionPool() {
        log.info("Connecting to Standalone Redis at: {}", redisUrl);
        try {
            GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(redisPoolMaxcConnections); // Maximum number of connections in the pool
            poolConfig.setMaxIdle(redisPoolMaxIdle);   // Maximum number of idle connections
            poolConfig.setMinIdle(redisPoolMinIdle);
            RedisURI redisUri = RedisURI.builder().withHost(redisUrl).withPort(redisPort)
                    .withAuthentication(redisUsername, redisPassword.toCharArray()).withSsl(true)
                    .withTimeout(Duration.ofSeconds(30)).build();
            RedisClient redisClient = RedisClient.create(redisUri);
            return ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig);
        } catch (Exception e) {
            log.error("Exception while creating redis standalone client", e);
            return null;
        }
    }


    @Override
    public void processElement(Map<String, Object> record, Context ctx, Collector<String> out) throws Exception {
        if (!record.containsKey("_key")) return;
        log.info("record: {}", record);
        buffer.add(record);
        if (buffer.size() >= batchSize) {
            flushToRedis();
        }
    }

    private void flushToRedis() throws Exception {
        if (buffer.isEmpty()) return;
        // Borrowing a Redis connection from the pool
        if (redisMode.equals(REDIS_CLUSTER_MODE) && null != redisClusterConnectionPool) {
            try (StatefulRedisClusterConnection<String, String> connection = redisClusterConnectionPool.borrowObject()) {
                RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = connection.async();
                // Using pipelining for bulk HSET writes
                List<CompletableFuture<?>> futures = new ArrayList<>();
                for (Map<String, Object> record : buffer) {
                    String hashKey = REDIS_KEY_SPACE_PREFIX + record.get("_key").toString();
                    for (String field : record.keySet())
                        futures.add(asyncCommands.hset(hashKey, field, record.get(field).toString()).toCompletableFuture());
                }
                // Waiting for all Redis operations to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                log.info("CLUSTER_REDIS_SINK: {}", buffer.size());
                buffer.clear();
            }
        } else {
            if (!redisMode.equals(REDIS_CLUSTER_MODE) && null != redisConnectionPool) {
                try (StatefulRedisConnection<String, String> connection = redisConnectionPool.borrowObject()) {
                    RedisAsyncCommands<String, String> asyncCommands = connection.async();
                    // Using pipelining for bulk HSET writes
                    List<CompletableFuture<?>> futures = new ArrayList<>();
                    for (Map<String, Object> record : buffer) {
                        String hashKey = REDIS_KEY_SPACE_PREFIX + record.get("_key").toString();
                        for (String field : record.keySet())
                            futures.add(asyncCommands.hset(hashKey, field, record.get(field).toString()).toCompletableFuture());
                    }
                    // Waiting for all Redis operations to complete
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                    log.info("REDIS_SINK: {}", buffer.size());
                    buffer.clear();
                }
            }
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (redisClusterConnectionPool != null)
            redisClusterConnectionPool.close();
        if (redisConnectionPool != null)
            redisConnectionPool.close();
    }
}

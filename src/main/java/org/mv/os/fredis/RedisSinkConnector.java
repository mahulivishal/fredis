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
    private Configs configs;

    public RedisSinkConnector(Configs configs) {
        this.configs = configs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Configure the connection pool
        if (configs.getRedisMode().equals(Constants.REDIS_CLUSTER_MODE))
            redisClusterConnectionPool = getRedisClusterConnectionPool();
        else
            redisConnectionPool = getRedisConnectionPool();
    }

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> getRedisClusterConnectionPool() {
        log.info("Connecting to Redis Cluster at: {}", configs.getRedisUrl());
        try {
            GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(configs.getRedisPoolMaxcConnections()); // Maximum number of connections in the pool
            poolConfig.setMaxIdle(configs.getRedisPoolMaxIdle());   // Maximum number of idle connections
            poolConfig.setMinIdle(configs.getRedisPoolMinIdle());
            String[] redisNodes = configs.getRedisUrl().split(",");
            List<RedisURI> redisNodeConnections = new ArrayList<>();
            for (String node : redisNodes) {
                RedisURI redisUri = RedisURI.builder().withHost(node).withPort(configs.getRedisPort())
                        .withAuthentication(configs.getRedisUsername(), configs.getRedisPassword().toCharArray()).withSsl(true).build();
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
    public void processElement(Map<String, Object> record, Context ctx, Collector<String> out) throws Exception {
        if (!record.containsKey("_key")) return;
        log.debug("record: {}", record);
        buffer.add(record);
        if (buffer.size() >= configs.getBatchSize()) {
            flushToRedis();
        }
    }

    private void flushToRedis() throws Exception {
        if (buffer.isEmpty()) return;
        // Borrowing a Redis connection from the pool
        if (configs.getRedisMode().equals(Constants.REDIS_CLUSTER_MODE) && null != redisClusterConnectionPool) {
            try (StatefulRedisClusterConnection<String, String> connection = redisClusterConnectionPool.borrowObject()) {
                RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = connection.async();
                // Using pipelining for bulk HSET writes
                List<CompletableFuture<?>> futures = new ArrayList<>();
                for (Map<String, Object> record : buffer) {
                    String hashKey = Constants.REDIS_KEY_SPACE_PREFIX + record.get("_key").toString();
                    for (String field : record.keySet())
                        futures.add(asyncCommands.hset(hashKey, field, record.get(field).toString()).toCompletableFuture());
                }
                // Waiting for all Redis operations to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                log.info("CLUSTER_REDIS_SINK: {}", buffer.size());
                buffer.clear();
            }
        } else {
            if (!configs.getRedisMode().equals(Constants.REDIS_CLUSTER_MODE) && null != redisConnectionPool) {
                try (StatefulRedisConnection<String, String> connection = redisConnectionPool.borrowObject()) {
                    RedisAsyncCommands<String, String> asyncCommands = connection.async();
                    // Using pipelining for bulk HSET writes
                    List<CompletableFuture<?>> futures = new ArrayList<>();
                    for (Map<String, Object> record : buffer) {
                        String hashKey = Constants.REDIS_KEY_SPACE_PREFIX + record.get("_key").toString();
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

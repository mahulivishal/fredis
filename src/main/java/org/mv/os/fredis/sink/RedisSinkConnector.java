package org.mv.os.fredis.sink;

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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.mv.os.fredis.config.Configs;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class RedisSinkConnector extends ProcessFunction<Map<String, Object>, Void> {

    private transient GenericObjectPool<StatefulRedisClusterConnection<String, String>> redisClusterConnectionPool;
    private transient GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool;
    private final Map<String, Map<String, Object>> latestEventMap = new HashMap<>();
    private final List<Map<String, Object>> eventsBuffer = new ArrayList<>();
    private static final String REDIS_KEY_SPACE_PREFIX = "vehicle_snapshot_";
    private static final String REDIS_CLUSTER_MODE = "cluster";
    private final Configs configs;

    private ValueState<Long> lastProcessedTimestamp;
    private ValueState<Integer> bufferCounter;

    public RedisSinkConnector(Configs config) {
        this.configs = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastProcessedTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastProcessed", Types.LONG));
        bufferCounter = getRuntimeContext().getState(new ValueStateDescriptor<>("bufferCounter", Types.INT));
        // Configure the connection pool
        if (configs.getRedisMode().equals(REDIS_CLUSTER_MODE))
            redisClusterConnectionPool = getRedisClusterConnectionPool();
        else
            redisConnectionPool = getRedisConnectionPool();
    }

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> getRedisClusterConnectionPool(){
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
        }catch (Exception e){
            log.error("Exception while creating redis cluster client", e);
            return null;
        }
    }

    private GenericObjectPool<StatefulRedisConnection<String, String>> getRedisConnectionPool(){
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
        }catch (Exception e){
            log.error("Exception while creating redis standalone client", e);
            return null;
        }
    }

    @Override
    public void processElement(Map<String, Object> incomingRecord, Context ctx, Collector<Void> out) throws Exception {
        if(!incomingRecord.containsKey("_key")) return;
        manageBuffer(incomingRecord);
        bufferCounter.update(bufferCounter.value() != null ? bufferCounter.value() + 1 : 1);
        if (bufferCounter.value() >= configs.getBatchSize())
            flushEvents(ctx);
    }

    /**
     * When the buffer is greater than 1, and we have multiple upsert requests per key, we only need to upsert the latest one
     * this function retrieves only the latest record per key if enableEventUpsertPerKey is set to true, this reduces the no of upserts to the DB and mitigates race-condition.
     * @param incomingRecord
     */
    private void manageBuffer(Map<String, Object> incomingRecord) {
        log.debug("incomingRecord: {}", incomingRecord);
        try {
            String key = Objects.requireNonNull(incomingRecord.get("_key"), "Record must contain '_key'").toString();
            String createdTspStr = Objects.requireNonNull(incomingRecord.get("createdTsp"), "Record must contain 'createdTsp'").toString();
            long createdTsp = Double.valueOf(createdTspStr).longValue();
            if(configs.isEnableEventUpsertPerKey()) {
                latestEventMap.compute(key, (k, existingRecord) -> {
                    if (existingRecord == null || createdTsp >= Double.valueOf(existingRecord.get("createdTsp").toString()).longValue())
                        return incomingRecord; // Keep the new record
                    return existingRecord; // Keep the existing record
                });
            } else
                eventsBuffer.add(incomingRecord);
        } catch (Exception e) {
            log.error("Exception in manageBuffer record: {}", incomingRecord, e);
        }
    }

    // Flushes events to Redis and resets the timers.
    private void flushEvents(Context ctx) throws Exception{
        Long startTime = System.currentTimeMillis();
        writeToRedis();
        Long endTime = System.currentTimeMillis();
        log.info("REDIS_SINK: batch_latency: {} ms", (endTime - startTime));
        bufferCounter.update(0); latestEventMap.clear(); eventsBuffer.clear();
        if (lastProcessedTimestamp.value() != null)
            ctx.timerService().deleteEventTimeTimer(lastProcessedTimestamp.value());
        if (configs.getSinkTimerIntervalInMS() != 0L)
            ctx.timerService().registerEventTimeTimer(System.currentTimeMillis() + configs.getSinkTimerIntervalInMS());
    }

    private void writeToRedis() throws Exception {
        List<Map<String, Object>> processedBuffer = !latestEventMap.isEmpty() ? new ArrayList<>(latestEventMap.values()): eventsBuffer;
        if(processedBuffer.isEmpty()) return;
        log.info("REDIS_SINK: processedBuffer: {}", processedBuffer.size());
        if(configs.getRedisMode().equals(REDIS_CLUSTER_MODE) && null != redisClusterConnectionPool) {
            try (StatefulRedisClusterConnection<String, String> connection = redisClusterConnectionPool.borrowObject()) {
                redisSinkInClusterMode(connection, processedBuffer);
            }
        } else {
            if(null != redisConnectionPool) {
                try (StatefulRedisConnection<String, String> connection = redisConnectionPool.borrowObject()) {
                    redisSinkInStandAloneMode(connection, processedBuffer);
                }
            }
        }
    }

    private void redisSinkInClusterMode(StatefulRedisClusterConnection<String, String> connection, List<Map<String, Object>> postProcessBuffer) {
        RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = connection.async();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (Map<String, Object> incomingRecord : postProcessBuffer) {
            String hashKey = REDIS_KEY_SPACE_PREFIX + incomingRecord.get("_key").toString();
            for (Map.Entry<String, Object> entry : incomingRecord.entrySet())
                futures.add(asyncCommands.hset(hashKey, entry.getKey(), entry.getValue().toString()).toCompletableFuture());
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void redisSinkInStandAloneMode(StatefulRedisConnection<String, String> connection, List<Map<String, Object>> postProcessBuffer) {
        RedisAsyncCommands<String, String> asyncCommands = connection.async();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (Map<String, Object> incomingRecord : postProcessBuffer) {
            String hashKey = REDIS_KEY_SPACE_PREFIX + incomingRecord.get("_key").toString();
            for (Map.Entry<String, Object> entry : incomingRecord.entrySet()) {
                futures.add(asyncCommands.hset(hashKey, entry.getKey(), entry.getValue().toString()).toCompletableFuture());
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisClusterConnectionPool != null)
            redisClusterConnectionPool.close();
        if (redisConnectionPool != null)
            redisConnectionPool.close();
    }

    /**
     * When the traffic isn't enough to fill the buffer, events are flushed to redis at regular intervals based on timers.
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
        log.info("REDIS_SINK: timer_triggered -- ");
        flushEvents(ctx);
    }
}
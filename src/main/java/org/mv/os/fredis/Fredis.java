package org.mv.os.fredis;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.mv.os.fredis.config.Configs;
import org.mv.os.fredis.sink.RedisSinkConnector;
import org.mv.os.fredis.sink.RedisStreamsSinkConnector;
import org.mv.os.fredis.source.RedisStreamSource;
import org.mv.os.fredis.utils.Event;
import org.mv.os.fredis.utils.EventMapper;
import org.mv.os.fredis.utils.NullFilter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Fredis {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String redisUrl = "localhost";
        final String redisUsername = "redis";
        final String redisPassword = "redis";
        final int redisPort = 6379;
        final int redisPoolMaxcConnections = 100;
        final int redisPoolMaxIdle = 10;
        final int redisPoolMinIdle = 5;
        final String redisMode = "cluster";
        int redisWriteBufferBatchSize = 100;
        int outOfOrdernessThresholdInSecs = 15;

        final MapFunction<String, Map<String, Object>> eventMapper = new EventMapper();
        Configs configs = Configs.builder().redisUrl(redisUrl).redisUsername(redisUsername).redisPassword(redisPassword)
                .redisPort(redisPort).redisPoolMaxcConnections(redisPoolMaxcConnections).redisPoolMaxIdle(redisPoolMaxIdle)
                .redisPoolMinIdle(redisPoolMinIdle).redisMode(redisMode).batchSize(redisWriteBufferBatchSize).build();

        // Example DataStream
        List<Event> events = new ArrayList<>();
        DataStream<String> dataStream = env.fromElements(events.toString())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessThresholdInSecs)));

        // Example Redis Sink Connector
        final ProcessFunction<Map<String, Object>, String> redisSinkConnector = new RedisSinkConnector(configs);
        dataStream.map(eventMapper).name("event-mapper").filter(new NullFilter<>())
                        .keyBy(obj -> obj.get("_key"))
                        .process(redisSinkConnector).name("redis-connector");

        // Example Redis Streams Sink Connector
        final ProcessFunction<Map<String, Object>, String> redisStreamsSinkConnector = new RedisStreamsSinkConnector(configs);
        dataStream.map(eventMapper).name("event-mapper").filter(new NullFilter<>())
                .keyBy(obj -> obj.get("_key"))
                .process(redisStreamsSinkConnector).name("redis-streams-connector");

        // Example Redis Streams Source Connector with Redis Streams Sink Connector
        List<String> redisStreamEvents = new ArrayList<>();
        RedisStreamSource redisSource = new RedisStreamSource(redisStreamEvents);
        DataStream<String> stream = env.fromSource(redisSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessThresholdInSecs)), "redis-stream-source");
        stream.map(eventMapper).name("redis-stream-event-mapper").filter(new NullFilter<>())
                .keyBy(obj -> obj.get("_key"))
                .process(redisStreamsSinkConnector).name("redis-streams-connector");

        env.execute("FREDIS - Flink Redis Sink Writer");
    }
}
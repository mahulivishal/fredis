package org.mv.os.fredis;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;

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

        final MapFunction<String, Map<String, Object>> eventMapper = new EventMapper();
        final ProcessFunction<Map<String, Object>, String> redisSinkConnector = new RedisSinkConnector(redisWriteBufferBatchSize, redisUrl, redisPort, redisPoolMaxcConnections, redisPoolMaxIdle, redisPoolMinIdle, redisUsername, redisPassword, redisMode);
        // Example DataStream
        List<Event> events = new ArrayList<>();
        DataStream<String> dataStream = env.fromElements(events.toString())
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));
        dataStream.map(eventMapper).name("event-mapper")
                        .keyBy(obj -> obj.get("_key"))
                        .process(redisSinkConnector).name("redis-connector");
        env.execute("FREDIS - Flink Redis Sink Writer");
    }
}
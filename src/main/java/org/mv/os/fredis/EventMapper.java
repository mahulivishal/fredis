package org.mv.os.fredis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class EventMapper implements MapFunction<String, Map<String, Object>> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> map(String event) throws Exception {
        // getOpenSearchDocument function only converts POJO to Hashmap, nothing OS specific
        Map<String, Object> redisEntry = new HashMap<>();
        redisEntry = mapper.convertValue(event, Map.class);
        return redisEntry;
    }
}

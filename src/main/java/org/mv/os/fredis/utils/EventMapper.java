package org.mv.os.fredis.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@Slf4j
public class EventMapper implements MapFunction<String, Map<String, Object>> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> map(String event) throws Exception {
        try {
            return mapper.convertValue(event, Map.class);
        }catch (Exception e){
            log.error("Exception while mapping event", e);
            return null;
        }
    }
}

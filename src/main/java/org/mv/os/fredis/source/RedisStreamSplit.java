package org.mv.os.fredis.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.connector.source.SourceSplit;

@Data
@AllArgsConstructor
public class RedisStreamSplit implements SourceSplit {
    private final String streamKey;
    private final String offset;

    @Override
    public String splitId() {
        return streamKey;
    }
}
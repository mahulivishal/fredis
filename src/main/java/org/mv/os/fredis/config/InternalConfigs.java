package org.mv.os.fredis.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
public class InternalConfigs {
    public static final int sourceReaderPollIntervalInMS = 100;
    public static final int readBufferMaxSize = 10;
    public static final String initialSplitOffset = "0-0";
}

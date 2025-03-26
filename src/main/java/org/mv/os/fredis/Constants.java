package org.mv.os.fredis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Constants {
    public static final String REDIS_KEY_SPACE_PREFIX = "fredis_";
    public static final String REDIS_STREAMS_KEY_SPACE_PREFIX = "fredis_streams";
    public static final String REDIS_CLUSTER_MODE = "cluster";
}

package org.mv.os.fredis.config;

import lombok.Data;

@Data
public class Constants {
    public static final String REDIS_KEY_SPACE_PREFIX = "fredis_";
    public static final String REDIS_STREAMS_KEY_SPACE_PREFIX = "fredis_streams_";
    public static final String REDIS_CLUSTER_MODE = "cluster";
}

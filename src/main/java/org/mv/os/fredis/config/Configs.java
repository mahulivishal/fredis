package org.mv.os.fredis.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Configs {
    int batchSize; // Number of records per bulk write
    String redisUrl;
    String redisUsername;
    String redisPassword;
    int redisPort;
    int redisPoolMaxcConnections;
    int redisPoolMaxIdle;
    int redisPoolMinIdle;
    String redisMode;
    int connectionTimeoutInSec;

}

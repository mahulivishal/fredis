package org.mv.os.fredis.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event implements Serializable {
    private String deviceId;
    private int soc;
    private float soh;
    private boolean isActive;
    private float speedInKmph;
}

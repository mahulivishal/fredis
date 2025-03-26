package org.mv.os.fredis.utils;

import org.apache.flink.api.common.functions.FilterFunction;

public class NullFilter<T> implements FilterFunction<T> {
    public boolean filter(T t) throws Exception {
        return t != null;
    }
}

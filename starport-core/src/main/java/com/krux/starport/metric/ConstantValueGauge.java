package com.krux.starport.metric;

import java.io.Serializable;

import com.codahale.metrics.Gauge;


public class ConstantValueGauge<T> implements Gauge<T>, Serializable {

    private T value;

    public ConstantValueGauge(T value) {
        this.value = value;
    }

    @Override
    public T getValue() {
        return value;
    }
}

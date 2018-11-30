package com.krux.starport.metric;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;


/**
 * This is a simple timer gauge has the advantage of not logging too many stats as the default
 * timers.
 */
public class SimpleTimerGauge implements Gauge<Long>, Serializable {

    private Long startNano;
    private Long stopNano;
    private TimeUnit defaultTimeUnit;

    public SimpleTimerGauge() {
        this(TimeUnit.NANOSECONDS);
    }

    public SimpleTimerGauge(TimeUnit defaultTimeUnit) {
        this.defaultTimeUnit = defaultTimeUnit;
        start();
    }

    public void start() {
        startNano = System.nanoTime();
        stopNano = null;
    }

    public void stop() {
        stopNano = System.nanoTime();
    }

    public boolean isStopped() {
        return stopNano != null;
    }

    public Long getElapsedNano() {
        if (stopNano == null) {
            return System.nanoTime() - startNano;
        }
        return stopNano - startNano;
    }

    public Long getElapsedTime(TimeUnit unit) {
        return unit.convert(getElapsedNano(), TimeUnit.NANOSECONDS);
    }

    public Long getElapsedTime() {
        return getElapsedTime(defaultTimeUnit);
    }

    @Override
    public Long getValue() {
        return getElapsedTime();
    }

}

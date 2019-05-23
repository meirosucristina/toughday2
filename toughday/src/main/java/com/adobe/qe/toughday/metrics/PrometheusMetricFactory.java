package com.adobe.qe.toughday.metrics;

import io.prometheus.client.SimpleCollector;

public interface PrometheusMetricFactory<T extends SimpleCollector> {

    T getPrometheusRepresentation(String phaseName, String testName);

    void updatePrometheusObject(Object value, T promObject, String phaseName, String testName);
}

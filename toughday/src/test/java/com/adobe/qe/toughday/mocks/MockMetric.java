package com.adobe.qe.toughday.mocks;

import com.adobe.qe.toughday.api.core.RunMap;
import com.adobe.qe.toughday.metrics.Metric;
import com.adobe.qe.toughday.metrics.PrometheusMetricFactory;
import io.prometheus.client.SimpleCollector;

public class MockMetric extends Metric {
    @Override
    public Object getValue(RunMap.TestStatistics testStatistics) {
        return null;
    }

    @Override
    public <T extends SimpleCollector> PrometheusMetricFactory<T> getPrometheusMetricFactory() {
        return null;
    }

    @Override
    public String getFormat() {
        return null;
    }

    @Override
    public String getUnitOfMeasure() {
        return null;
    }

}

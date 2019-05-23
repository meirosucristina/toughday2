package com.adobe.qe.toughday.publishers.prometheus;

import com.adobe.qe.toughday.api.core.AbstractTest;
import com.adobe.qe.toughday.api.core.MetricResult;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.metrics.Metric;
import com.adobe.qe.toughday.metrics.PrometheusMetricFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.SimpleCollector;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class PrometheusMetricsCollector {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private Map<String, SimpleCollector> prometheusMetrics = new HashMap<>();
    private Map<String, Metric> metrics = new HashMap<>();
    private CollectorRegistry registry = new CollectorRegistry();
    private Phase phase;

    private String getPrometheusMetricName(String metricName, String currentPhaseName, String testName) {
        return metricName + "_" + currentPhaseName + "_" + testName;
    }

    /**
     * Constructor.
     * @param metrics : list of metrics that must be pushed to the Prometheus push gateway.
     */
    public PrometheusMetricsCollector(List<Metric> metrics, Phase phase) {
        this.phase = phase;

        metrics.forEach(metric -> this.metrics.put(metric.getName(), metric));
        System.out.println("Collecting metrics to be pushed to prometheus gateway");

        for (Metric m : metrics) {
            for (AbstractTest test : phase.getTestSuite().getTests()) {
                System.out.println("getting prom representation");
                PrometheusMetricFactory factory = m.getPrometheusMetricFactory();
                if (factory != null) {
                    SimpleCollector promMetric = m.getPrometheusMetricFactory().getPrometheusRepresentation(phase.getName(),
                            test.getFullName());

                    if (promMetric != null) {
                        System.out.println("Prom metric != null -> adding it at key " +
                                getPrometheusMetricName(m.getName(), phase.getName(), test.getFullName()));
                        this.prometheusMetrics.put(getPrometheusMetricName(m.getName(), phase.getName(), test.getFullName()), promMetric);
                    }
                }
            }
        }

        System.out.println("Successfully collected all the metrics.");
        registerPrometheusMetrics();
    }

    private void registerPrometheusMetrics() {
        prometheusMetrics.values().forEach(prometheusMetric -> {
            System.out.println("Registering prom metric " + prometheusMetric.toString());
            prometheusMetric.register(registry);
        });
    }

    /**
     * Method used to update the value of the prometheus objects created for TD metrics.
     * @param results : map containing as key the name of the test and as value a list with the updates for each metric
     *                specified in the configuration.
     */
    public void updatePrometheusObject(Map<String, List<MetricResult>> results) {
        LOG.info("Updating metrics...");

        results.forEach((testName, resultList) ->
                resultList.forEach(result -> {
                    String identifier = getPrometheusMetricName(result.getName(), phase.getName(), testName);
                    System.out.println("identifier is " + identifier);
                    if (prometheusMetrics.containsKey(identifier)) {
                        System.out.println("Found metric to update");
                        this.metrics.get(result.getName()).getPrometheusMetricFactory().updatePrometheusObject(result.getValue(),
                                prometheusMetrics.get(identifier), phase.getName(), testName);
                    }

                }));

        LOG.info("Successfully updated metrics");
    }

    /**
     * Getter for the collector registry.
     */
    public CollectorRegistry getRegistry() {
        return this.registry;
    }

}

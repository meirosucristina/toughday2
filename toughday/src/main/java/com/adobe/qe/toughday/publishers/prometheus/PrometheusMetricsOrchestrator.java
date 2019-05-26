package com.adobe.qe.toughday.publishers.prometheus;

import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.metrics.Failed;
import com.adobe.qe.toughday.metrics.Metric;
import com.adobe.qe.toughday.metrics.Passed;
import com.adobe.qe.toughday.metrics.Skipped;
import io.prometheus.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class responsible for converting TD metrics into Prometheus objects to be pushed into the prometheus
 * push gateway.
 */
public class PrometheusMetricsOrchestrator {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private Map<Class<? extends Metric>, PrometheusMetricsFactory> classMetricToPrometheusMetricFactory = new HashMap<>();
    private Map<String, CollectorRegistry> registryPerPhase = new HashMap<>();
    // key = name of the phase; value: map (key = name of the metric; value = metric class)
    private Map<String, Map<String, Class<? extends Metric>>> metricToClass = new HashMap<>();
    private Phase currentPhase = null;

    private List<Class<? extends Metric>> whitelist = new ArrayList<Class<? extends Metric>>() {{
        add(Passed.class);
        add(Failed.class);
        add(Skipped.class);
    }};


    private String getPrometheusIdentifierForMetric(String metricName, String phaseName, String testName) {
        return metricName + "." + phaseName + "." + testName;
    }

    /**
     * Constructor.
     * @param phases : list of phases to be executed. A new CollectorRegistry object will be created for each phase,
     *               containing the Prometheus data objects associated with the metrics to be generated when executing
     *               it.
     */
    public PrometheusMetricsOrchestrator(List<Phase> phases) {
        CounterFactory counterFactory = new CounterFactory();

        // set factory type for each supported metric
        classMetricToPrometheusMetricFactory.put(Passed.class, counterFactory);
        classMetricToPrometheusMetricFactory.put(Failed.class, counterFactory);
        classMetricToPrometheusMetricFactory.put(Skipped.class, counterFactory);

        phases.forEach(phase -> {
            metricToClass.put(phase.getName(), new HashMap<>());
            CollectorRegistry collectorRegistry = new CollectorRegistry();

            phase.getMetrics()
                .stream()
                .filter(metric -> whitelist.contains(metric.getClass()))
                .forEach(metric -> {
                    metricToClass.get(phase.getName()).put(metric.getName(), metric.getClass());

                    // create the collector registry containing all the prometheus objects exposed for the current phase
                    phase.getTestSuite().getTests().forEach(test -> {
                        SimpleCollector promObject = classMetricToPrometheusMetricFactory.get(metric.getClass())
                                .createPrometheusRepresentationForMetric(metric.getName(), phase.getName(),
                                        test.getFullName(), metric.getName());
                        promObject.register(collectorRegistry);
                    });

                });

            registryPerPhase.put(phase.getName(), collectorRegistry);
        });
    }

    /**
     * Method used to update the value of the prometheus metrics.
     * @param metricName : name of the metric to be updated.
     * @param testName : name of test that the metric is associated with
     * @param newValue : the new value of the metric
     */
    public void updatePrometheusObject(String metricName, String testName, Object newValue) {
        Class<? extends Metric> metricClass = metricToClass.get(currentPhase.getName()).get(metricName);

        if (!whitelist.contains(metricClass)) {
            LOG.warn("Metric " + metricName + " is currently not supported in Prometheus.");
            return;
        }

        this.classMetricToPrometheusMetricFactory.get(metricClass).updatePrometheusObject(metricName, testName, newValue);
    }

    /**
     * Setter for the current phase of the execution
     * @param phase
     */
    public void setCurrentPhase(Phase phase) {
        this.currentPhase = phase;
    }

    /**
     * Getter for the collector registry containing all the Prometheus metrics to be exposed,
     */
    public CollectorRegistry getCollectorRegistryForCurrentPhase() {
        return registryPerPhase.get(currentPhase.getName());
    }

    /**
     * Common interface for the classes responsible for creating a certain type of Prometheus data object (Counter,
     * Gauge, etc).
     * @param <T> : type of the Prometheus data objects to be created
     */
    interface PrometheusMetricsFactory<T extends SimpleCollector> {
        /**
         * Creates the Prometheus data object which can represent a TD metric described by the given parameters.
         * @param metricName : name of the metric
         * @param phaseName : name of the phase
         * @param testName : name of the test
         * @param help : help message to be used when creating the Prometheus data object
         */
        T createPrometheusRepresentationForMetric(String metricName, String phaseName, String testName,
                                                  String help);

        /**
         * Method responsible for updating the value of the Prometheus metric identified by the given parameters.
         * @param metricName : name of the metric
         * @param testName : name of the test for which the metric was generated
         * @param newValue : the new value of the Prometheus metric
         */
        void updatePrometheusObject(String metricName, String testName, Object newValue);
    }

    private class CounterFactory implements PrometheusMetricsFactory<Counter> {
        Map<String, Counter> counterObjects = new HashMap<>();

        @Override
        public Counter createPrometheusRepresentationForMetric(String metricName, String phaseName,
                                                               String testName, String help) {
            LOG.info("[Counter factory] Creating prometheus counter for metric " + metricName);
            Counter counter =  Counter.build()
                    .name(metricName)
                    .labelNames("phase", "test")
                    .help(help)
                    .create();
            counterObjects.put(getPrometheusIdentifierForMetric(metricName, phaseName, testName), counter);
            return counter;
        }

        @Override
        public void updatePrometheusObject(String metricName, String testName, Object newValue) {
            String prometheusObjectIdentfier = getPrometheusIdentifierForMetric(metricName, currentPhase.getName(), testName);
            Counter counter = counterObjects.get(prometheusObjectIdentfier);

            Double oldValue = counter.labels(currentPhase.getName(), testName).get();
            Double valueToInc = Double.valueOf(newValue.toString()) - oldValue;

            counter.labels(currentPhase.getName(), testName).inc(valueToInc);
        }
    }
}

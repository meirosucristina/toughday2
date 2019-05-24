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

public class PrometheusMetricsOrchestrator {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private Map<Class, PrometheusMetricsFactory> classMetricToPrometheusMetricFactory = new HashMap<>();
    private Map<String, CollectorRegistry> registryPerPhase = new HashMap<>();
    // key = name of the phase; value: map (key = name of the metric; value = metric class)
    private Map<String, Map<String, Class<? extends Metric>>> metricToClass = new HashMap<>();
    private Phase currentPhase = null;

    private List<Class<? extends Metric>> supportedMetricTypes = new ArrayList<Class<? extends Metric>>() {{
        add(Passed.class);
        add(Failed.class);
        add(Skipped.class);
    }};


    private String getPrometheusIdentifierForMetric(String metricName, String phaseName, String testName) {
        return metricName + "." + phaseName + "." + testName;
    }

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
                .filter(metric -> supportedMetricTypes.contains(metric.getClass()))
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
            LOG.info("[PrometheusMetricOrchestrator Constructor] Successfully created all collector registries.");
        });
    }

    public void updatePrometheusObject(String metricName, String testName) {
        Class<? extends Metric> metricClass = metricToClass.get(currentPhase.getName()).get(metricName);

        if (!supportedMetricTypes.contains(metricClass)) {
            LOG.info("Metric " + metricName + " is currently not supported in Prometheus.");
            return;
        }

        this.classMetricToPrometheusMetricFactory.get(metricClass).updatePrometheusObject(metricName, testName);
    }

    public void setCurrentPhase(Phase phase) {
        this.currentPhase = phase;
    }

    public CollectorRegistry getCollectorRegistryForCurrentPhase() {
        if (registryPerPhase.get(currentPhase.getName()) == null) {
            LOG.warn("Null registry for current phase!");
        }
        return registryPerPhase.get(currentPhase.getName());
    }

    interface PrometheusMetricsFactory<T extends SimpleCollector> {
        T createPrometheusRepresentationForMetric(String metricName, String phaseName, String testName,
                                                  String help);

        void updatePrometheusObject(String metricName, String testName);
    }

    private class CounterFactory implements PrometheusMetricsFactory<Counter> {
        Map<String, Counter> counterObjects = new HashMap<>();

        @Override
        public Counter createPrometheusRepresentationForMetric(String metricName, String phaseName,
                                                               String testName, String help) {
            LOG.info("[Counter factory] Creating prometheus counter. Phase=" + phaseName + ". Metric= " + metricName);
            Counter counter =  Counter.build()
                    .name(metricName)
                    .labelNames("phase", "test")
                    .help(help)
                    .create();
            counterObjects.put(getPrometheusIdentifierForMetric(metricName, phaseName, testName), counter);
            LOG.info("[Counter factory] Successfully created prometheus counter.");
            return counter;
        }

        @Override
        public void updatePrometheusObject(String metricName, String testName) {
            LOG.info("[Counter factory] Updating  metric" + metricName);
            String prometheusObjectIdentfier = getPrometheusIdentifierForMetric(metricName, currentPhase.getName(), testName);
            counterObjects.get(prometheusObjectIdentfier).labels(currentPhase.getName(), testName).inc();
            LOG.info("[Counter factory] Successfully updated metric" + metricName);

        }
    }
}

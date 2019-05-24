package com.adobe.qe.toughday.publishers.prometheus;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.api.core.MetricResult;
import com.adobe.qe.toughday.api.core.Publisher;
import com.adobe.qe.toughday.api.core.benchmark.TestResult;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.PushGateway;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PrometheusPublisher extends Publisher {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private static final String DEFAULT_PUSH_GATEWAY_PORT = "9091";
    private static final String DEFAULT_JOB_NAME = "TD_JOB";

    private String pushGatewayHostname;
    private int pushGatewayPort = Integer.parseInt("9091");
    private String jobName;
    private PushGateway pushGateway = null;
    private PrometheusMetricsOrchestrator prometheusMetricsOrchestrator = null;

    @ConfigArgSet(desc = "Hostname at which the prometheus push gateway can be accessed.")
    public void setPushGatewayHostname(String pushGatewayHostname) {
        this.pushGatewayHostname = pushGatewayHostname;
    }

    @ConfigArgGet
    public String getPushGatewayHostname() {
        return this.pushGatewayHostname;
    }

    @ConfigArgSet(required = false, desc = "Port on which the prometheus push gateway is listening",
            defaultValue = DEFAULT_PUSH_GATEWAY_PORT)
    public void setPushGatewayPort(String pushGatewayPort) {
        this.pushGatewayPort = Integer.parseInt(pushGatewayPort);
    }

    @ConfigArgGet
    public String getPushGatewayPort() {
        return String.valueOf(pushGatewayPort);
    }

    @ConfigArgSet(required = false, desc = "Name of the job to be used when pushing metrics into the prometheus push" +
            "gateway", defaultValue = DEFAULT_JOB_NAME)
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setPrometheusMetricsOrchestrator(PrometheusMetricsOrchestrator prometheusMetricsOrchestrator) {
        this.prometheusMetricsOrchestrator = prometheusMetricsOrchestrator;
    }

    @ConfigArgGet
    public String getJobName() {
        return this.jobName;
    }

    @Override
    protected void doPublishAggregatedIntermediate(Map<String, List<MetricResult>> testResults) {
        if (this.pushGateway == null) {
            LOG.info("Creating push gateway");
            this.pushGateway = new PushGateway("my-release-prometheus-pushgateway.default.svc.cluster.local" + ":" + "9091");
            LOG.info("Successfully created prometheus push gateway");
        }

        testResults.forEach((testName, metricResultList) -> {
            metricResultList.forEach(metricResult -> {
                LOG.info("Updating prometheus object for metric: " + metricResult.getName());
                this.prometheusMetricsOrchestrator.updatePrometheusObject(metricResult.getName(), testName);
                LOG.info("Successfully updated prometheus object for metric " + metricResult.getName());
            });
        });

        CollectorRegistry collectorRegistry = new CollectorRegistry();
        Counter counter = Counter.build().name("blabla_counter").help("bla bla help").create();
        counter.register(collectorRegistry);

        // push data to prometheus gateway
        try {
            LOG.info("Pushing prometheus objects...");
            this.pushGateway.push(collectorRegistry, jobName);
            LOG.info("Successfully pushed prometheus objects");
        } catch (IOException e) {
            System.out.println("Failed to push data to prometheus gateway");
        } catch (Throwable t) {
            LOG.info("ERRORRRRR");
            LOG.warn(t.getMessage());
        }

    }

    @Override
    protected void doPublishAggregatedFinal(Map<String, List<MetricResult>> map) {

    }

    @Override
    protected void doPublishRaw(Collection<TestResult> testResults) { }

    @Override
    public void finish() {

    }
}

package com.adobe.qe.toughday.publishers.prometheus;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.api.core.MetricResult;
import com.adobe.qe.toughday.api.core.Publisher;
import com.adobe.qe.toughday.api.core.benchmark.TestResult;
import com.adobe.qe.toughday.publishers.CSVPublisher;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Prometheus publisher.
 */
public class PrometheusPublisher extends Publisher {
    private static final Logger LOG = LoggerFactory.getLogger(CSVPublisher.class);

    private static final String DEFAULT_PUSHGATEWAY_PORT = "9091";
    private static final String DEFAULT_JOB_NAME = "TD_JOB";

    private String pushGatewayHostname;
    private int pushGatewayPort;
    private String jobName;
    private PushGateway pushGateway = null;
    private PrometheusMetricsCollector prometheusMetricsCollector;

    @ConfigArgSet(desc = "Hostname at which the prometheus push gateway can be accessed.")
    public void setPushGatewayHostname(String pushGatewayHostname) {
        this.pushGatewayHostname = pushGatewayHostname;
    }

    @ConfigArgGet
    public String getPushGatewayHostname() {
        return this.pushGatewayHostname;
    }

    @ConfigArgSet(required = false, desc = "Port on which the prometheus push gateway is listening",
            defaultValue = DEFAULT_PUSHGATEWAY_PORT)
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

    public String getJobName() {
        return this.jobName;
    }

    @Override
    protected void doPublishAggregatedIntermediate(Map<String, List<MetricResult>> map) {
        if (this.pushGateway == null) {
            System.out.println("Creating push gateway");
            this.pushGateway = new PushGateway(pushGatewayHostname + ":" + pushGatewayPort);
            System.out.println("Successfully created prometheus push gateway");

        }

        System.out.println("Updating prometheus objects");
        prometheusMetricsCollector.updatePrometheusObject(map);
        System.out.println("Successfully updated prometheus objects");

        try {
            System.out.println("pushing updated to gateway");
            pushGateway.push(prometheusMetricsCollector.getRegistry(), this.jobName);
            System.out.println("Successfully pushed updates to gateway");
        } catch (IOException e) {
            LOG.warn("Failed to push metrics to prometheus gateway. Error encountered: " + e.getMessage());
        }

    }

    public void setPrometheusMetricsCollector(PrometheusMetricsCollector prometheusMetricsCollector) {
        LOG.info("Setting prometheus publisher collector...");
        this.prometheusMetricsCollector = prometheusMetricsCollector;
    }

    @Override
    protected void doPublishAggregatedFinal(Map<String, List<MetricResult>> map) {

    }

    @Override
    protected void doPublishRaw(Collection<TestResult> collection) {

    }

    @Override
    public void finish() {

    }
}

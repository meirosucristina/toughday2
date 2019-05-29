package com.adobe.qe.toughday.publishers;

import com.adobe.qe.toughday.api.annotations.ConfigArgGet;
import com.adobe.qe.toughday.api.annotations.ConfigArgSet;
import com.adobe.qe.toughday.api.core.MetricResult;
import com.adobe.qe.toughday.api.core.Publisher;
import com.adobe.qe.toughday.api.core.benchmark.TestResult;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.runmodes.ConstantLoad;
import com.adobe.qe.toughday.internal.core.engine.runmodes.Normal;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Publisher used by the agents in the K8s cluster to publish their individual results. Aggregated
 * metrics could then by determined using tools like Grafana.
 */
public class InfluxDbPublisher extends Publisher {
    private static final String DEFAULT_DATABASE_NAME = "td-on-k8s";
    private static final String DEFAULT_PORT = "3036";
    private static final String DEFAULT_TABLE_NAME = "testResults";
    private static final String DEFAULT_K8S_NAMESPACE = "default";
    private String agentIP;
    int totalPoints = 0;

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbPublisher.class);

    private String influxName;
    private int port = Integer.parseInt(DEFAULT_PORT);
    private String databaseName = DEFAULT_DATABASE_NAME;
    private String tableName = DEFAULT_TABLE_NAME;
    private String k8sNamespace = DEFAULT_K8S_NAMESPACE;
    private InfluxDB influxDB;
    private Engine engine;
    private boolean setup = false;
    public boolean printDriverData = false;
    public String driverId;


    @ConfigArgSet(required = false, defaultValue = "3036", desc = "Port used for connecting to the Influx database.")
    public void setPort(String port) {
        if (!port.matches("^[0-9]+$")) {
            throw new IllegalArgumentException("Port must be a number");
        }

        this.port = Integer.parseInt(port);
    }

    @ConfigArgGet
    public int getPort() {
        return this.port;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public void setAgentIP(String agentIP) {
        this.agentIP = agentIP;
    }

    @ConfigArgSet(desc = "Influx service name")
    public void setInfluxName(String influxName) {
        this.influxName = influxName;
    }

    @ConfigArgGet
    public String getInfluxName() {
        return this.influxName;
    }

    @ConfigArgSet(required = false, defaultValue = "td-on-k8s", desc = "Name of the database in which" +
            "the results are stored.")
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @ConfigArgGet
    public String getDatabaseName() {
        return this.databaseName;
    }

    @ConfigArgSet(required = false, defaultValue = DEFAULT_TABLE_NAME, desc = "The name of the table" +
            " in which the results are stored.")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @ConfigArgGet
    public String getTableName() {
        return this.tableName;
    }

    @ConfigArgSet(required = false, defaultValue = DEFAULT_K8S_NAMESPACE, desc = "The K8s namespace" +
            " where the InfluxDb was deployed.")
    public void setK8sNamespace(String k8sNamespace) {
        this.k8sNamespace = k8sNamespace;
    }

    @ConfigArgGet
    public String getK8sNamespace() {
        return this.k8sNamespace;
    }

    private void verifyConnection() {
        Pong response = this.influxDB.ping();
        if (response.getVersion().equalsIgnoreCase("unknown")) {
            LOG.warn("Unable to ping database. Connection was lost.");
        }

        // TODO: try reestablishing the connection. Log error in case of multiple consecutive failures
    }

    private void setup() {
        String connectionString = "http://10.244.0.166:8086";

        this.influxDB = InfluxDBFactory.connect(connectionString);
        // avoid log messages at the console
        this.influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
        verifyConnection();
        // createDatabase();

        this.setup = true;
    }

    private void createDatabase() {
        this.influxDB.createDatabase(this.databaseName);
    }


    @Override
    protected void doPublishAggregatedIntermediate(Map<String, List<MetricResult>> results) {
        // not supported by this kind of publisher
    }

    @Override
    protected void doPublishAggregatedFinal(Map<String, List<MetricResult>> results) {
        // not supported by this kind of publisher
    }

    @Override
    protected void doPublishRaw(Collection<TestResult> testResults) {
        if (!setup) {
            setup();
        }

        BatchPoints batchPoints = BatchPoints.database(this.databaseName)
                .retentionPolicy("autogen")
                .build();

        for (TestResult testResult : testResults) {

            int threads;
            String label;

            if (engine.getCurrentPhase().getRunMode() instanceof Normal) {
                label = "nrThreads";
                threads = ((Normal)engine.getCurrentPhase().getRunMode()).getActiveThreads();
            } else {
                label = "load";
                threads = ((ConstantLoad)engine.getCurrentPhase().getRunMode()).getCurrentLoad();
            }


            Point point = Point.measurement(this.tableName)
                    .time(testResult.getStartTimestamp(), TimeUnit.MILLISECONDS)
                    .addField("name", testResult.getTestFullName())
                    .addField("status", testResult.getStatus().toString())
                    .addField("thread", testResult.getThreadId())
                    .addField("start timestamp", testResult.getFormattedStartTimestamp())
                    .addField("end timestamp", testResult.getFormattedEndTimestamp())
                    .addField("duration", testResult.getDuration() / 1000)
                    .addField("agentIdField", this.agentIP)
                    .tag("agentId", this.agentIP)
                    .addField(label, threads)
                    .build();
            batchPoints.point(point);

        }

        if (!testResults.isEmpty()) {
            this.influxDB.write(batchPoints);
        }


        if (printDriverData) {
            if (engine == null || engine.getConfiguration() == null) {
                LOG.info("NULL CONFIG => SKIPPING...");
            }
            try {
                BatchPoints batchPoints1 = BatchPoints.database(this.databaseName)
                        .retentionPolicy("autogen")
                        .build();
                Point point1 = Point.measurement("driver")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("driverId", driverId)
                        .addField("NrRequestSentToAgents", engine.getConfiguration().nrMessagesSentToAgents)
                        .addField("NrRedistributionRequests", engine.getConfiguration().nrRedistributionRequests)
                        .addField("NrMessagesSentToMaster", engine.getConfiguration().nrMessagesSentToMaster)
                        .build();
                batchPoints1.point(point1);

                this.influxDB.write(batchPoints1);
            } catch (Exception e) {
                LOG.info("FAILED TO WRITE DRIVER DATA " + e.getMessage());
            }
        }
    }

    @Override
    public void finish() {
        this.influxDB.close();
    }

}

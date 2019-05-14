package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.MasterHeartbeatTask;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.HeartbeatTask;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;

import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.HTTP_REQUEST_RETRIES;
import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.URL_PREFIX;
import static spark.Spark.*;

/**
 * Driver component for the cluster.
 */
public class Driver {
    // routes
    public static final String EXECUTION_PATH = "/config";
    private static final String REGISTER_PATH = "/registerAgent";
    private static final String PHASE_FINISHED_BY_AGENT_PATH = "/phaseFinished";
    private static final String HEALTH_PATH = "/health";
    private static final String SAMPLE_CONTENT_ACK_PATH = "/contentAck";
    private static final String MASTER_ELECTION_PATH = "/masterElection";
    private static final String ASK_FOR_UPDATES_PATH = "/driverUpdates";
    private static final String GET_NR_DRIVERS_PATH = "/getNrDrivers";

    private static final String HOSTNAME = "driver";
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final HttpUtils httpUtils = new HttpUtils();
    private final TaskBalancer taskBalancer = TaskBalancer.getInstance();
    private DistributedPhaseMonitor distributedPhaseMonitor = new DistributedPhaseMonitor();
    private Configuration configuration;
    private DriverState driverState;
    private MasterElection masterElection;
    private ScheduledFuture<?> scheduledFuture;
    private final RequestProcessorDispatcher requestProcessorDispatcher = RequestProcessorDispatcher.getInstance();

    public DistributedPhaseMonitor getDistributedPhaseMonitor() {
        return this.distributedPhaseMonitor;
    }

    public TaskBalancer getTaskBalancer() {
        return this.taskBalancer;
    }

    public DriverState getDriverState() {
        return this.driverState;
    }

    public MasterElection getMasterElection() {
        return this.masterElection;
    }

    public ScheduledExecutorService getHeartbeatScheduler() {
        return this.heartbeatScheduler;
    }

    public Driver(Configuration configuration) {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            this.driverState = new DriverState(hostname, configuration);
        } catch (UnknownHostException e) {
            System.exit(-1);
        }

        this.masterElection = new MasterElection(this.driverState.getNrDrivers());
        this.masterElection.electMasterWhenDriverJoinsTheCluster(this);

        LOG.info("Driver " + this.driverState.getHostname() + " elected as master " + this.driverState.getMasterId());
    }

    public static String getExecutionPath(String driverIdentifier, String port, boolean forwardReq) {
        return HttpUtils.URL_PREFIX + driverIdentifier + ":" + port + Driver.EXECUTION_PATH + "?forward=" + forwardReq;
    }

    /**
     * Returns the http URL that should be used by the agents to get the number of drivers running in the cluster.
     */
    /*public static String getGetNrDriversPath() {
        return URL_PREFIX + HOSTNAME + ":" + SVC_PORT + GET_NR_DRIVERS_PATH;
    } */

    /**
     * Returns the http URL that should be used by the agents whenever they finished executing the task received from
     * the driver.
     */
    public static String getPhaseFinishedByAgentPath(String driverIdentifier, String port, boolean forwardReq) {
        return URL_PREFIX + driverIdentifier + ":" + port + PHASE_FINISHED_BY_AGENT_PATH + "?forward=" + forwardReq;
    }

    /**
     * Returns the http URL that should be used by the agent chosen to install the TD sample content package for
     * informing the driver that the installation was completed successfully.
     */
    public static String getSampleContentAckPath(String driverIdentifier, String port) {
        return URL_PREFIX + driverIdentifier + ":" + port + SAMPLE_CONTENT_ACK_PATH;
    }

    /**
     * Returns the http URL that should be used by the driver which detected that the current Master died to inform all
     * the other drivers running in the cluster that the master election process must be triggered.
     * @param driverHostname : hostname of the driver exposing this http endpoint
     */
    public static String getMasterElectionPath(String driverHostname, String port) {
        return URL_PREFIX + driverHostname + ":" + port + MASTER_ELECTION_PATH;
    }

    public static String getAgentRegisterPath(String driverIdentifier, String port, boolean forwardReq) {
        return URL_PREFIX + driverIdentifier + ":" + port + REGISTER_PATH + "?forward=" + forwardReq;
    }

    public static String getHealthPath(String driverHostName) {
        return URL_PREFIX + driverHostName + ":4567" + HEALTH_PATH;
    }

    /**
     * Returns the http URL that should be used by the drivers to get information about the current state of the
     * execution.
     * @param driverHostName : hostname of the driver component exposing this http endpoint
     */
    public static String getAskForUpdatesPath(String driverHostName) {
        return URL_PREFIX + driverHostName + ":4567" + ASK_FOR_UPDATES_PATH;
    }

    public ExecutorService getExecutorService() {
        return this.executorService;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void finishAgents() {
        this.driverState.getRegisteredAgents().forEach(agentIp -> {
            LOG.info("[Driver] Finishing agent " + agentIp);
            HttpResponse response =
                    httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, "", Agent.getFinishPath(agentIp), HTTP_REQUEST_RETRIES);
            if (response == null) {
                // the assumption is that the agent will be killed when he fails to respond to heartbeat request
                LOG.warn("Driver could not finish the execution on agent " + agentIp + ".");
            }
        });

        this.driverState.getRegisteredAgents().clear();
    }

    public void finishDistributedExecution() {
        this.executorService.shutdownNow();
        // finish tasks
        this.heartbeatScheduler.shutdownNow();

        finishAgents();
    }

    public void scheduleHeartbeatTask() {
        // we should periodically send heartbeat messages from driver to all the agents
        heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(this.driverState.getRegisteredAgents(), this.distributedPhaseMonitor,
                        this.configuration, this.driverState.getDriverConfig()),
                0, this.driverState.getDriverConfig().getDistributedConfig().getHeartbeatIntervalInSeconds(), TimeUnit.SECONDS);
    }

    private void scheduleMasterHeartbeatTask() {
        // we should periodically send heartbeat messages from slaves to check id the master is still running
        this.scheduledFuture = this.heartbeatScheduler.scheduleAtFixedRate(new MasterHeartbeatTask(this), 0,
                GlobalArgs.parseDurationToSeconds("10s"), TimeUnit.SECONDS);
    }

    /**
     * Method used for cancelling the periodic task of sending heartbeat messages to the driver running as Master in the
     * cluster.
     */
    public void cancelMasterHeartBeatTask() {
        if (this.scheduledFuture != null) {
            if(!this.scheduledFuture.cancel(true) && !this.scheduledFuture.isDone()) {
                LOG.warn("Could not cancel task used to periodically send heartbeat messages to the Master.");
            }
        }
    }

    /**
     * Starts the execution of the driver.
     */
    public void run() {
        RequestProcessorDispatcher dispatcher = RequestProcessorDispatcher.getInstance();
        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> dispatcher.getRequestProcessor(this)
                .processExecutionRequest(request, response, this)
        ));

        /* health check http endpoint */
        get(HEALTH_PATH, ((request, response) -> "Healthy"));

        /* expose http endpoint to allow agents to get the number of drivers running in the cluster */
        get(GET_NR_DRIVERS_PATH, ((request, response) -> String.valueOf(this.getDriverState().getNrDrivers())));

        /* http endpoint used by the agent installing the sample content to announce if the installation was
         * successful or not. */
        post(SAMPLE_CONTENT_ACK_PATH, ((request, response) ->
                    dispatcher.getRequestProcessor(this).acknowledgeSampleContentSuccessfulInstallation(request, this, response)));

        /* expose http endpoint to allow agents to announce when they finished executing the current phase */
        post(PHASE_FINISHED_BY_AGENT_PATH, ((request, response) ->
                dispatcher.getRequestProcessor(this).processPhaseCompletionAnnouncement(request)));

        /* expose http endpoint for triggering the master election process */
        post(MASTER_ELECTION_PATH, ((request, response) ->
                dispatcher.getRequestProcessor(this).processMasterElectionRequest(request, this)));

        /* expose http endpoint for sending information about the current state of the distributed execution */
        get(ASK_FOR_UPDATES_PATH, ((request, response) -> dispatcher.getRequestProcessor(this).processUpdatesRequest(request)));

        /* expose http endpoint for registering new agents in the cluster */
        post(REGISTER_PATH, (request, response) -> dispatcher.getRequestProcessor(this).processRegisterRequest(request));

        if (this.driverState.getCurrentState() == DriverState.State.MASTER) {
            scheduleHeartbeatTask();
        } else if (this.driverState.getCurrentState() == DriverState.State.SLAVE) {
            scheduleMasterHeartbeatTask();
        }
    }
}

package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.MasterHeartbeatTask;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.tasks.HeartbeatTask;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.HTTP_REQUEST_RETRIES;
import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.URL_PREFIX;
import static com.adobe.qe.toughday.internal.core.engine.Engine.logGlobal;
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

    private static final String HOSTNAME = "driver";
    private static final String PORT = "80";
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
    private final Object object = new Object();

    public DriverState getDriverState() {
        return this.driverState;
    }

    public MasterElection getMasterElection() {
        return this.masterElection;
    }

    public Driver(Configuration configuration) {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            this.driverState = new DriverState(hostname, configuration);
        } catch (UnknownHostException e) {
            System.exit(-1);
        }

        this.masterElection = new MasterElection(this.driverState.getNrDrivers());

        // check if there is already a master running in the cluster
        this.masterElection.collectUpdatesFromAllDrivers(this);

        // if no master was detected, trigger the master election process
        this.driverState.getMasterIdLock().readLock().lock();
        if (this.driverState.getMasterId() == -1) {
            this.driverState.getMasterIdLock().readLock().unlock();
            this.masterElection.electMaster(this);
        } else {
            this.driverState.getMasterIdLock().readLock().unlock();
        }

        LOG.info("Driver " + this.driverState.getHostname() + " elected as master " + this.driverState.getMasterId());

        if (this.driverState.getId() == this.driverState.getMasterId()) {
            LOG.info("Running as MASTER");
            this.driverState.setCurrentState(DriverState.State.MASTER);
        } else {
            LOG.info("Running as SLAVE");
            this.driverState.setCurrentState(DriverState.State.SLAVE);
        }

        // TODO: schedule pods to ask for information from all the other drivers running in the cluster
    }

    /**
     * Returns the http URL that should be used by the agents in order to register themselves.
     */
    public static String getAgentRegisterPath() {
        return URL_PREFIX + HOSTNAME + ":" + PORT + REGISTER_PATH;
    }

    /**
     * Returns the http URL that should be used by the agents whenever they finished executing the task received from
     * the driver.
     */
    public static String getPhaseFinishedByAgentPath() {
        return URL_PREFIX + HOSTNAME + ":" + PORT + PHASE_FINISHED_BY_AGENT_PATH;
    }

    /**
     * Returns the http URL that should be used by the agent chosen to install the TD sample content package for
     * informing the driver that the installation was completed successfully.
     */
    public static String getSampleContentAckPath() {
        return URL_PREFIX + HOSTNAME + ":" + PORT + SAMPLE_CONTENT_ACK_PATH;
    }

    public static String getMasterElectionPath(String driverHostname) {
        return URL_PREFIX + driverHostname + ":4567" + MASTER_ELECTION_PATH;
    }

    private String getAgentRegisterPath(String driverHostname, boolean forwardReq) {
        return URL_PREFIX + driverHostname + ":4567" + REGISTER_PATH + "?forward=" + forwardReq;
    }

    public static String getHealthPath(String driverHostName) {
        return URL_PREFIX + driverHostName + ":4567" + HEALTH_PATH;
    }

    public static String getAskForUpdatesPath(String driverHostName) {
        return URL_PREFIX + driverHostName + ":4567" + ASK_FOR_UPDATES_PATH;
    }

    private void waitForSampleContentToBeInstalled() {
        synchronized (object) {
            try {
                object.wait();
            } catch (InterruptedException e) {
                LOG.error("Failed to install ToughDay sample content package. Execution will be stopped.");
                finishDistributedExecution();
                System.exit(-1);
            }
        }
    }

    private void installToughdayContentPackage(Configuration configuration) {
        logGlobal("Installing ToughDay 2 Content Package...");
        GlobalArgs globalArgs = configuration.getGlobalArgs();

        if (globalArgs.getDryRun() || !globalArgs.getInstallSampleContent()) {
            return;
        }

        HttpResponse agentResponse = null;
        List<String> agentsCopy = new ArrayList<>(this.driverState.getRegisteredAgents());

        while (agentResponse == null && agentsCopy.size() > 0) {
            // pick one agent to install the sample content
            String agentIpAddress = agentsCopy.remove(0);
            String URI = Agent.getInstallSampleContentPath(agentIpAddress);
            LOG.info("Installing sample content request was sent to agent " + agentIpAddress);

            YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
            String yamlTask = dumpConfig.generateConfigurationObject();

            agentResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, yamlTask, URI, HTTP_REQUEST_RETRIES);
        }

        // we should wait until the we receive confirmation that the sample content was successfully installed
        waitForSampleContentToBeInstalled();

        logGlobal("Finished installing ToughDay 2 Content Package.");
        globalArgs.setInstallSampleContent("false");

    }

    private void mergeDistributedConfigParams(Configuration configuration) {
        if (this.driverState.getDriverConfig().getDistributedConfig().getHeartbeatIntervalInSeconds() ==
                this.configuration.getDistributedConfig().getHeartbeatIntervalInSeconds()) {

            this.driverState.getDriverConfig().getDistributedConfig().merge(configuration.getDistributedConfig());
            return;
        }

        // cancel heartbeat task and reschedule it with the new period
        this.heartbeatScheduler.shutdownNow();
        this.driverState.getDriverConfig().getDistributedConfig().merge(configuration.getDistributedConfig());
        scheduleHeartbeatTask();
    }

    private void finishAgents() {
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

    private void finishDistributedExecution() {
        this.executorService.shutdownNow();
        // finish tasks
        this.heartbeatScheduler.shutdownNow();

        finishAgents();
    }

    private void handleExecutionRequest(Configuration configuration) {
        installToughdayContentPackage(configuration);
        mergeDistributedConfigParams(configuration);

        PhaseSplitter phaseSplitter = new PhaseSplitter();

        for (Phase phase : configuration.getPhases()) {
            try {
                Map<String, Phase> tasks = phaseSplitter.splitPhase(phase, new ArrayList<>(this.driverState.getRegisteredAgents()));
                this.distributedPhaseMonitor.setPhase(phase);

                for (String agentIp : this.driverState.getRegisteredAgents()) {
                    configuration.setPhases(Collections.singletonList(tasks.get(agentIp)));

                    // convert configuration to yaml representation
                    YamlDumpConfiguration dumpConfig = new YamlDumpConfiguration(configuration);
                    String yamlTask = dumpConfig.generateConfigurationObject();

                    /* send query to agent and register running task */
                    String URI = Agent.getSubmissionTaskPath(agentIp);
                    HttpResponse response = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, yamlTask, URI, HTTP_REQUEST_RETRIES);

                    if (response != null) {
                        this.distributedPhaseMonitor.registerAgentRunningTD(agentIp);
                        LOG.info("Task was submitted to agent " + agentIp);
                    } else {
                        /* the assumption is that the agent is no longer active in the cluster and he will fail to respond
                         * to the heartbeat request sent by the driver. This will automatically trigger process of
                         * redistributing the work
                         * */
                        LOG.info("Task\n" + yamlTask + " could not be submitted to agent " + agentIp +
                                ". Work will be rebalanced once the agent fails to respond to heartbeat request.");
                    }
                }

                // al execution queries were sent => set phase execution start time
                this.distributedPhaseMonitor.setPhaseStartTime(System.currentTimeMillis());

                // we should wait until all agents complete the current tasks in order to execute phases sequentially
                if (!this.distributedPhaseMonitor.waitForPhaseCompletion(3)) {
                    break;
                }

                LOG.info("Phase " + phase.getName() + " finished execution successfully.");

            } catch (CloneNotSupportedException e) {
                LOG.error("Phase " + phase.getName() + " could not de divided into tasks to be sent to the agents.", e);

                LOG.info("Finishing agents");
                finishAgents();

                System.exit(-1);
            }
        }

        finishDistributedExecution();
    }

    private void scheduleHeartbeatTask() {
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
        /* expose http endpoint for running TD with the given configuration */
        post(EXECUTION_PATH, ((request, response) -> {
            String yamlConfiguration = request.body();
            Configuration configuration = new Configuration(yamlConfiguration);
            this.configuration = configuration;

            // handle execution in a different thread to be able to quickly respond to this request
            this.executorService.submit(() -> handleExecutionRequest(configuration));

            return "";
        }));

        /* health check http endpoint */
        get(HEALTH_PATH, ((request, response) -> "Healthy"));

        /* http endpoint used by the agent installing the sample content to announce if the installation was
         * successful or not. */
        post(SAMPLE_CONTENT_ACK_PATH, ((request, response) -> {
            boolean installed = Boolean.parseBoolean(request.body());

            if (!installed) {
                LOG.error("Failed to install the ToughDay sample content package. Execution will be stopped.");
                finishDistributedExecution();
                System.exit(-1);
            }

            synchronized (object) {
                object.notify();
            }

            return "";
        }));

        /* expose http endpoint to allow agents to announce when they finished executing the current phase */
        post(PHASE_FINISHED_BY_AGENT_PATH, ((request, response) -> {
            String agentIp = request.body();

            LOG.info("Agent " + agentIp + " finished executing the current phase.");
            this.distributedPhaseMonitor.removeAgentFromActiveTDRunners(agentIp);

            return "";
        }));

        post(MASTER_ELECTION_PATH, ((request, response) -> {
            int failedDriverId = Integer.parseInt(request.body());

            // check if this news was already processed
            if (this.masterElection.isCandidateInvalid(failedDriverId)) {
                return "";
            }

            LOG.info("Driver was informed that the current master (id: " + failedDriverId + ") died");
            this.masterElection.markCandidateAsInvalid(failedDriverId);

            // pick a new leader
            this.masterElection.electMaster(this);
            LOG.info("New master was elected: " + this.driverState.getMasterId());

            return "";
        }));

        get(ASK_FOR_UPDATES_PATH, ((request, response) -> {
            String destinationDriver = request.body();
            LOG.info("Driver " + destinationDriver + " has requested updates about the state of the cluster ");

            // build information to send to the driver that recently joined the cluster
            DriverUpdateInfo driverUpdateInfo = new DriverUpdateInfo(this.driverState.getHostname(),
                    this.driverState.getCurrentState(), this.masterElection.getInvalidCandidates(), this.driverState.getRegisteredAgents());

            ObjectMapper objectMapper = new ObjectMapper();
            String yamlUpdateInfo = objectMapper.writeValueAsString(driverUpdateInfo);
            LOG.info("Create YAML update info: " + yamlUpdateInfo);

            // set response
            response.body(yamlUpdateInfo);
            return "";
        }));

        /* expose http endpoint for registering new agents in the cluster */
        post(REGISTER_PATH, (request, response) -> {
            String agentIp = request.body();

            LOG.info("[driver] Registered agent with ip " + agentIp);
            if (!request.queryParams().contains("forward") || !request.queryParams("forward").equals("false")) {
                /* register new agents to all the drivers running in the cluster */
                for (int i = 0; i < this.driverState.getNrDrivers(); i++) {
                    /* skip current driver and inactive drivers */
                    if (i == this.driverState.getId() || this.driverState.getInactiveDrivers().contains(i)) {
                        continue;
                    }

                    LOG.info(this.driverState.getHostname() + ": sending agent register request for agent " + agentIp + "" +
                            "to driver " + this.driverState.getPathForId(i));
                    HttpResponse regResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, agentIp,
                            getAgentRegisterPath(this.driverState.getPathForId(i), false), HTTP_REQUEST_RETRIES);
                    if (regResponse == null) {
                        // the assumption is that the new driver will receive the full list of active agents after being restarted
                        LOG.info("Driver " + this.driverState.getHostname() + "failed to send register request for agent " + agentIp +
                                "to driver " + this.driverState.getPathForId(i));
                    }
                }
            }

            if (!this.distributedPhaseMonitor.isPhaseExecuting()) {
                this.driverState.registerAgent(agentIp);
                LOG.info("[driver] active agents " + this.driverState.getRegisteredAgents().toString());
                return "";
            }

            this.taskBalancer.scheduleWorkRedistributionProcess(distributedPhaseMonitor, this.driverState.getRegisteredAgents(),
                    configuration, this.driverState.getDriverConfig().getDistributedConfig(), agentIp, true);

            return "";
        });

        if (this.driverState.getCurrentState() == DriverState.State.MASTER) {
            scheduleHeartbeatTask();
        } else if (this.driverState.getCurrentState() == DriverState.State.SLAVE) {
            scheduleMasterHeartbeatTask();
        }
    }
}

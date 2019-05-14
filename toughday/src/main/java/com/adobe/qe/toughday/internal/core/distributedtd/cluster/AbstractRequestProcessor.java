package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.HTTP_REQUEST_RETRIES;

public abstract class AbstractRequestProcessor implements RequestProcessor {
    protected DriverState driverState;
    protected DistributedPhaseMonitor distributedPhaseMonitor;
    protected TaskBalancer taskBalancer;
    protected Configuration configuration;
    protected MasterElection masterElection;
    protected final HttpUtils httpUtils = new HttpUtils();
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    public AbstractRequestProcessor(DriverState driverState, DistributedPhaseMonitor distributedPhaseMonitor,
                                    TaskBalancer taskBalancer, MasterElection masterElection) {
        this.driverState = driverState;
        this.distributedPhaseMonitor = distributedPhaseMonitor;
        this.taskBalancer = taskBalancer;
        this.masterElection = masterElection;
    }

    @Override
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    protected List<String> getDriverPathsForRedirectingRequests(Driver currentDriver) {
        return IntStream.rangeClosed(0, currentDriver.getDriverState().getNrDrivers() - 1).boxed()
                .filter(id -> id != currentDriver.getDriverState().getId()) // exclude current driver
                .map(id -> currentDriver.getDriverState().getPathForId(id))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    public String processRegisterRequest(Request request) {
        String agentIp = request.body();

        LOG.info("[driver] Registered agent with ip " + agentIp);
        if (request.queryParams("forward").equals("true")) {
            /* register new agents to all the drivers running in the cluster */
            for (int i = 0; i < this.driverState.getNrDrivers(); i++) {
                /* skip current driver and inactive drivers */
                if (i == this.driverState.getId() || this.driverState.getInactiveDrivers().contains(i)) {
                    continue;
                }

                LOG.info(this.driverState.getHostname() + ": sending agent register request for agent " + agentIp + "" +
                        "to driver " + this.driverState.getPathForId(i));
                HttpResponse regResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, agentIp,
                        Driver.getAgentRegisterPath(this.driverState.getPathForId(i), HttpUtils.SPARK_PORT, false), HTTP_REQUEST_RETRIES);
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
    }

    @Override
    public String processUpdatesRequest(Request request) throws JsonProcessingException {
        LOG.info("Driver has requested updates about the state of the cluster.");

        // build information to send to the driver that recently joined the cluster
        DriverUpdateInfo driverUpdateInfo = new DriverUpdateInfo(this.driverState.getId(),
                this.driverState.getCurrentState(), this.masterElection.getInvalidCandidates(),
                this.driverState.getRegisteredAgents());

        ObjectMapper objectMapper = new ObjectMapper();
        String yamlUpdateInfo = objectMapper.writeValueAsString(driverUpdateInfo);
        LOG.info("Create YAML update info: " + yamlUpdateInfo);

        // set response
        return yamlUpdateInfo;
    }

    @Override
    public String processMasterElectionRequest(Request request, Driver currentDriver) {
        int failedDriverId = Integer.parseInt(request.body());

        // check if this news was already processed
        if (this.masterElection.isCandidateInvalid(failedDriverId)) {
            return "";
        }

        LOG.info("Driver was informed that the current master (id: " + failedDriverId + ") died");
        this.masterElection.markCandidateAsInvalid(failedDriverId);

        // pick a new leader
        this.masterElection.electMaster(currentDriver);
        LOG.info("New master was elected: " + this.driverState.getMasterId());

        return "";
    }

    @Override
    public String processPhaseCompletionAnnouncement(Request request) {
        String agentIp = request.body();

        LOG.info("Agent " + agentIp + " finished executing the current phase.");
        this.distributedPhaseMonitor.removeAgentFromActiveTDRunners(agentIp);

        /* if this is the first driver receiving this type of request, forward it to all the other drivers running in
         * the cluster.
         */
        if (request.queryParams("forward").equals("true")) {
            for (int i = 0; i < this.driverState.getNrDrivers(); i++) {
                /* skip current driver and inactive drivers */
                if (i == this.driverState.getId()) {
                    continue;
                }

                LOG.info(this.driverState.getHostname() + ": sending agent announcement for phase completion " +
                        agentIp + "" + "to driver " + this.driverState.getPathForId(i));
                HttpResponse response = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, agentIp,
                        Driver.getPhaseFinishedByAgentPath(this.driverState.getPathForId(i), HttpUtils.SPARK_PORT, false),
                        HTTP_REQUEST_RETRIES);

                if (response == null) {
                    // the assumption is that the new driver will receive the full list of active agents after being restarted
                    LOG.info("Driver " + this.driverState.getHostname() + "failed to send announcement for phase "
                            + "of agent " + agentIp + "to driver " + this.driverState.getPathForId(i));
                }

            }

        }
            return "";
    }


    @Override
    public String processExecutionRequest(Request request, Response response, Driver currentDriver) throws Exception {
        String yamlConfiguration = request.body();
        LOG.info("Received execution request for TD configuration:\n");
        LOG.info(yamlConfiguration);

        // save TD configuration which must be executed in distributed mode
        this.configuration = new Configuration(yamlConfiguration);
        currentDriver.setConfiguration(configuration);

        // send TD configuration to all the other drivers running in the cluster
        if (request.queryParams("forward").equals("true")) {
            List<String> forwardPaths = this.getDriverPathsForRedirectingRequests(currentDriver);
            forwardPaths.forEach(forwardPath -> {
                LOG.info("Forwarding execution request to driver " + forwardPath);
                HttpResponse driverResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, request.body(),
                        Driver.getExecutionPath(forwardPath, HttpUtils.SPARK_PORT, false), HttpUtils.HTTP_REQUEST_RETRIES);
                if (driverResponse == null) {
                    /* the assumption is that the driver will fail to respond to heartbeat request and will receive this
                     * information after being restarted.
                     */
                    LOG.warn("Unable to forward execution request to " + forwardPath);
                }
            });
        }

        return "";
    }
}

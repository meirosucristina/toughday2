package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.config.GlobalArgs;
import com.adobe.qe.toughday.internal.core.config.parsers.yaml.YamlDumpConfiguration;
import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import com.adobe.qe.toughday.internal.core.distributedtd.splitters.PhaseSplitter;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import org.apache.http.HttpResponse;
import spark.Request;
import spark.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils.HTTP_REQUEST_RETRIES;
import static com.adobe.qe.toughday.internal.core.engine.Engine.logGlobal;

public class MasterRequestProcessor extends AbstractRequestProcessor {
    private final Object object = new Object();
    private static MasterRequestProcessor instance = null;

    public static MasterRequestProcessor getInstance(Driver driver) {
        if (instance == null) {
            instance = new MasterRequestProcessor(driver.getDriverState(), driver.getDistributedPhaseMonitor(),
                    driver.getTaskBalancer(), driver.getMasterElection());
        }

        return instance;
    }

    private MasterRequestProcessor(DriverState driverState, DistributedPhaseMonitor distributedPhaseMonitor,
                                  TaskBalancer taskBalancer, MasterElection masterElection) {
        super(driverState, distributedPhaseMonitor, taskBalancer, masterElection);
    }

    private void waitForSampleContentToBeInstalled(Driver currentDriver) {
        synchronized (object) {
            try {
                object.wait();
            } catch (InterruptedException e) {
                LOG.error("Failed to install ToughDay sample content package. Execution will be stopped.");
                currentDriver.finishDistributedExecution();
                System.exit(-1);
            }
        }
    }

    private void installToughdayContentPackage(Configuration configuration, Driver currentDriver) {
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
        waitForSampleContentToBeInstalled(currentDriver);

        logGlobal("Finished installing ToughDay 2 Content Package.");
        globalArgs.setInstallSampleContent("false");

    }

    private void mergeDistributedConfigParams(Configuration configuration, Driver currentDriver) {
        if (this.driverState.getDriverConfig().getDistributedConfig().getHeartbeatIntervalInSeconds() ==
                this.configuration.getDistributedConfig().getHeartbeatIntervalInSeconds()) {

            this.driverState.getDriverConfig().getDistributedConfig().merge(configuration.getDistributedConfig());
            return;
        }

        // cancel heartbeat task and reschedule it with the new period
        currentDriver.getHeartbeatScheduler().shutdownNow();
        this.driverState.getDriverConfig().getDistributedConfig().merge(configuration.getDistributedConfig());
        currentDriver.scheduleHeartbeatTask();
    }


    private void handleExecutionRequest(Configuration configuration, Driver currentDriver) {
        installToughdayContentPackage(configuration, currentDriver);
        mergeDistributedConfigParams(configuration, currentDriver);

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
                currentDriver.finishAgents();

                System.exit(-1);
            }
        }

        currentDriver.finishDistributedExecution();
    }

    @Override
    public String acknowledgeSampleContentSuccessfulInstallation(Request request, Driver currentDriver, Response response) {
        boolean installed = Boolean.parseBoolean(request.body());

        if (!installed) {
            LOG.error("Failed to install the ToughDay sample content package. Execution will be stopped.");
            currentDriver.finishDistributedExecution();
            System.exit(-1);
        }

        synchronized (this.object) {
            this.object.notify();
        }

        return "";
    }

    @Override
    public String processExecutionRequest(Request request, Response response, Driver currentDriver) throws Exception {
        super.processExecutionRequest(request, response, currentDriver);

        // handle execution in a different thread to be able to quickly respond to this request
        currentDriver.getExecutorService().submit(() -> handleExecutionRequest(configuration, currentDriver));

        return "";
    }
}

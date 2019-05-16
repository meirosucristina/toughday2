package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.requests;

import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.Driver;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.DriverState;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.MasterElection;
import com.adobe.qe.toughday.internal.core.distributedtd.redistribution.TaskBalancer;
import org.apache.http.HttpResponse;
import spark.Request;
import spark.Response;

public class SlaveRequestProcessor extends AbstractRequestProcessor {
    private static SlaveRequestProcessor instance = null;

    public static SlaveRequestProcessor getInstance(Driver driver) {
        if (instance == null) {
            instance = new SlaveRequestProcessor(driver.getDriverState(), driver.getDistributedPhaseMonitor(),
                    driver.getTaskBalancer(), driver.getMasterElection());
        }

        return instance;
    }

    private SlaveRequestProcessor(DriverState driverState, DistributedPhaseMonitor distributedPhaseMonitor,
                                  TaskBalancer taskBalancer, MasterElection masterElection) {
        super(driverState, distributedPhaseMonitor, taskBalancer, masterElection);
    }

    @Override
    public String processRegisterRequest(Request request, Driver currentDriver) {
        super.processRegisterRequest(request, currentDriver);

        String agentIp = request.body();
        this.driverState.registerAgent(agentIp);
        LOG.info("[driver] Registered agent with ip " + agentIp);
        LOG.info("[driver] active agents " + this.driverState.getRegisteredAgents().toString());

        return "";
    }

    @Override
    public String acknowledgeSampleContentSuccessfulInstallation(Request request, Driver currentDriver, Response response) {
        LOG.info("Slave redirecting sample content ack to the master...");
        // another driver should be chosen for this responsibility
        if (this.driverState.getMasterId() == -1) {
            response.status(503);
        }
        // request should be forwarded to the current master
        HttpResponse masterResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, request.body(),
                Driver.getSampleContentAckPath(this.driverState.getPathForId(this.driverState.getMasterId()), HttpUtils.SPARK_PORT),
                HttpUtils.HTTP_REQUEST_RETRIES);

        if (masterResponse == null) {
            response.status(503);
        }

        return "";
    }

    @Override
    public String processExecutionRequest(Request request, Response response, Driver currentDriver) throws Exception {
        super.processExecutionRequest(request, response, currentDriver);

        // slaves will assume that the execution started successfully
        currentDriver.getDistributedPhaseMonitor().setPhase(currentDriver.getConfiguration().getPhases().get(0));
        // the assumption is that the master is responsible for installing TD sample content
        currentDriver.getConfiguration().getGlobalArgs().setInstallSampleContent("false");

        return "";
    }
}

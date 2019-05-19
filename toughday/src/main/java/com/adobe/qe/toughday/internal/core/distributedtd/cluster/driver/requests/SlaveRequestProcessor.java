package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.requests;

import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.Driver;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.DriverState;
import org.apache.http.HttpResponse;
import spark.Request;
import spark.Response;

/**
 * Specifies how a slave will process the HTTP requests received from the other drivers or the agents running in the
 * cluster.
 */
public class SlaveRequestProcessor extends AbstractRequestProcessor {
    private static SlaveRequestProcessor instance = null;

    /**
     * Returns an instance of this class.
     * @param driver : the driver instance that will use this class for processing HTTP requests.
     */
    public static SlaveRequestProcessor getInstance(Driver driver) {
        if (instance == null || !instance.driverInstance.equals(driver)) {
            instance = new SlaveRequestProcessor(driver);
        }

        return instance;
    }

    private SlaveRequestProcessor(Driver driverInstance) {
        super(driverInstance);
    }

    @Override
    public String processRegisterRequest(Request request, Driver driverInstance) {
        super.processRegisterRequest(request, driverInstance);

        String agentIp = request.body();
        this.driverInstance.getDriverState().registerAgent(agentIp);
        LOG.info("[driver] Registered agent with ip " + agentIp);
        LOG.info("[driver] active agents " + this.driverInstance.getDriverState().getRegisteredAgents().toString());

        return "";
    }

    @Override
    public String acknowledgeSampleContentSuccessfulInstallation(Request request, Driver driverInstance, Response response) {
        DriverState driverState = driverInstance.getDriverState();

        LOG.info("Slave redirecting sample content ack to the master...");
        // another driver should be chosen for this responsibility
        if (driverState.getMasterId() == -1) {
            response.status(503);
        }
        // request should be forwarded to the current master
        HttpResponse masterResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD, request.body(),
                Driver.getSampleContentAckPath(driverState.getPathForId(driverState.getMasterId()), HttpUtils.SPARK_PORT),
                HttpUtils.HTTP_REQUEST_RETRIES);

        if (masterResponse == null) {
            response.status(503);
        }

        return "";
    }

    @Override
    public String processExecutionRequest(Request request, Driver driverInstance) throws Exception {
        super.processExecutionRequest(request, driverInstance);

        // slaves will assume that the execution started successfully
        driverInstance.getDistributedPhaseMonitor().setPhase(driverInstance.getConfiguration().getPhases().get(0));
        // the assumption is that the master is responsible for installing TD sample content
        driverInstance.getConfiguration().getGlobalArgs().setInstallSampleContent("false");

        return "";
    }
}

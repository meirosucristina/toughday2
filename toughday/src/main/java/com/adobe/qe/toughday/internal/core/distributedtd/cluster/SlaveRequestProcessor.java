package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.distributedtd.DistributedPhaseMonitor;
import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
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

    private SlaveRequestProcessor(DriverState driverState, DistributedPhaseMonitor distributedPhaseMonitor, TaskBalancer taskBalancer, MasterElection masterElection) {
        super(driverState, distributedPhaseMonitor, taskBalancer, masterElection);
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
}

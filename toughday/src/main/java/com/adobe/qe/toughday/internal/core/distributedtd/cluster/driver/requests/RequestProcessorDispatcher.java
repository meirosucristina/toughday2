package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.requests;

import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.Driver;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.DriverState;

public class RequestProcessorDispatcher {
    private static RequestProcessorDispatcher instance = null;

    private RequestProcessorDispatcher() { }

    public static RequestProcessorDispatcher getInstance() {
        if (instance == null) {
            instance = new RequestProcessorDispatcher();
        }

        return instance;
    }

    public RequestProcessor getRequestProcessor(Driver driver) {
        if (driver.getDriverState().getCurrentState() == DriverState.State.MASTER) {
            return MasterRequestProcessor.getInstance(driver);
        } else {
            return SlaveRequestProcessor.getInstance(driver);
        }

    }
}

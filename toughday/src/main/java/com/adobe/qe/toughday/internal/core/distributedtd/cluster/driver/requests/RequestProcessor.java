package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.requests;

import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.Driver;
import com.fasterxml.jackson.core.JsonProcessingException;
import spark.Request;
import spark.Response;

public interface RequestProcessor {
    String processRegisterRequest(Request request, Driver currentDriver);

    String processUpdatesRequest(Request request, Driver currentDriver) throws JsonProcessingException;

    String processMasterElectionRequest(Request request, Driver currentDriver);

    String processPhaseCompletionAnnouncement(Request request);

    String acknowledgeSampleContentSuccessfulInstallation(Request request, Driver currentDriver, Response response);

    String processExecutionRequest(Request request, Response response, Driver currentDriver) throws Exception;

}

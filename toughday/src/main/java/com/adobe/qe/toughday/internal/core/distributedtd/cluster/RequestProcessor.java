package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.fasterxml.jackson.core.JsonProcessingException;
import spark.Request;
import spark.Response;

public interface RequestProcessor {
    String processRegisterRequest(Request request);

    String processUpdatesRequest(Request request) throws JsonProcessingException;

    String processMasterElectionRequest(Request request, Driver currentDriver);

    String processPhaseCompletionAnnouncement(Request request);

    String acknowledgeSampleContentSuccessfulInstallation(Request request, Driver currentDriver, Response response);

    String processExecutionRequest(Request request, Response response, Driver currentDriver) throws Exception;

    void setConfiguration(Configuration configuration);

}

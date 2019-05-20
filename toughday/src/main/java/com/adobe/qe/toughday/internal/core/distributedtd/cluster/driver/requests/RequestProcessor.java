package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.requests;

import com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver.Driver;
import com.fasterxml.jackson.core.JsonProcessingException;
import spark.Request;
import spark.Response;

/**
 * Common interface for handling http requests received by the driver components running in the cluster.
 */
public interface RequestProcessor {
    /**
     * Method responsible for processing the request of registering a new Agent which joined the cluster recently.
     * @param request : http request received from the Agent.
     * @param driverInstance : the Driver instance processing this request
     */
    String processRegisterRequest(Request request, Driver driverInstance);

    /**
     * Method responsible for processing the request sent by the drivers running in the cluster to ask for updates
     * regarding the current state of the cluster. This method is called whenever a new Driver component joins the
     * cluster.
     * @param request : http request received from a Driver
     * @param driverInstance : The driver instance processing this request
     * @return Yaml representation of the current state of the cluster (generated by dumping the DriverUpdateInfo class)
     */
    String processUpdatesRequest(Request request, Driver driverInstance) throws JsonProcessingException;

    /**
     * Method responsible for processing the request sent by a driver to announce that the master election process must
     * be triggered.
     * @param request : http request used for triggering the master election process.
     * @param driverInstance : the Driver instance processing this request.
     */
    String processMasterElectionRequest(Request request, Driver driverInstance);

    /**
     * Method responsible for processing the request sent by an agent to announce that the Phase received as a task from
     * the driver was successfully executed.
     * @param request : http request sent by an agent which completed the execution of a phase.
     */
    String processPhaseCompletionAnnouncement(Request request);

    /**
     * Method responsible for processing the request sent by the agent responsible to install the TD sample content
     * package to announce that the installation was successfully completed.
     * @param request : http request sent by the agent.
     * @param driverInstance : the Driver instance processing this request
     * @param response
     * @return
     */
    String acknowledgeSampleContentSuccessfulInstallation(Request request, Driver driverInstance, Response response);

    /**
     * Method responsible for processing the request used to trigger the distributed execution of ToughDay.
     * @param request : http request
     * @param driverInstance : the Driver instance processing this request
     * @throws Exception : if the Configuration for running ToughDay cannot be successfully built and dumped as a YAML
     * string.
     */
    String processExecutionRequest(Request request, Driver driverInstance) throws Exception;

    String processHeartbeatRequest(Request request, Driver driverInstance);

}

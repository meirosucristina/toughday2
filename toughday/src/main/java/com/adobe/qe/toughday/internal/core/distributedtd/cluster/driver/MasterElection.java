package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver;

import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.Agent;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MasterElection {
    private int nrDrivers;
    private Queue<Integer> candidates;
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    public MasterElection(int nrDrivers) {
        this.nrDrivers = nrDrivers;
        this.candidates = IntStream.rangeClosed(0, nrDrivers - 1).boxed()
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
    }

    public void markCandidateAsInvalid(int candidateId) {
        this.candidates.remove(candidateId);
    }

    public boolean isCandidateInvalid(int candidateId) {
        return !this.candidates.contains(candidateId);
    }

    public Queue<Integer> getInvalidCandidates() {
        Queue<Integer> invalidCandidates = IntStream.rangeClosed(0, nrDrivers - 1).boxed()
                .collect(Collectors.toCollection(LinkedList::new));
        invalidCandidates.removeAll(this.candidates);

        return invalidCandidates;
    }

    private void before() {
        // restore all valid candidates in case there is no option;
        if (candidates.isEmpty()) {
            LOG.info("Resetting list of candidates to be considered for master election");
            this.candidates = IntStream.rangeClosed(0, nrDrivers - 1).boxed().collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        }
    }

    public void electMasterWhenDriverJoinsTheCluster(Driver newDriver) {
        // check if there is already a master running in the cluster
        collectUpdatesFromAllDrivers(newDriver);
        DriverState driverState = newDriver.getDriverState();

        // if no master was detected, trigger the master election process
        driverState.getMasterIdLock().readLock().lock();
        if (driverState.getMasterId() == -1) {
            driverState.getMasterIdLock().readLock().unlock();
            electMaster(newDriver);
            return;
        }

        driverState.getMasterIdLock().readLock().unlock();
        after(newDriver);
    }

    public void electMaster(Driver driver) {
        before();

        // set new master
        LOG.info("Electing new master " + candidates.stream().min(Integer::compareTo).get());

        driver.getDriverState().getMasterIdLock().writeLock().lock();
        driver.getDriverState().setMasterId(candidates.stream().min(Integer::compareTo).get());
        driver.getDriverState().getMasterIdLock().writeLock().unlock();

        after(driver);
    }

    private List<String> getIdleAgents(Driver driver) {
        HttpUtils httpUtils = new HttpUtils();
        List<String> idleAgents = new ArrayList<>();

        driver.getDriverState().getRegisteredAgents()
            .forEach(agentIp -> {
                HttpResponse agentResponse = httpUtils.sendHttpRequest(HttpUtils.GET_METHOD, "",
                        Agent.getGetStatusPath(agentIp), HttpUtils.HTTP_REQUEST_RETRIES);
                if (agentResponse != null) {
                    try {
                        String status = EntityUtils.toString(agentResponse.getEntity());
                        if (status.equals(Agent.Status.IDLE.toString())) {
                            idleAgents.add(agentIp);
                        }
                    } catch (IOException e) {
                        LOG.warn("Could not check status of agent. Current phase might not be executed with the "
                                + "desired configuration.");
                    }
                }
            });

        return idleAgents;
    }



    private void after(Driver driver) {
        // cancel heartbeat task for the diver elected as the new master
        driver.getDriverState().getMasterIdLock().readLock().lock();

        // running as master
        if (driver.getDriverState().getMasterId() == driver.getDriverState().getId()) {
            LOG.info("Running as MASTER");

            driver.getDriverState().setCurrentState(DriverState.State.MASTER);
            LOG.info("Stopping master heartbeat periodic task since this driver was elected as master.");
            driver.cancelMasterHeartBeatTask();

            /* check if work redistribution is required */
            Queue<String> agentsRunningTD = driver.getDriverState().getRegisteredAgents();
            List<String> idleAgents = getIdleAgents(driver);
            agentsRunningTD.removeAll(idleAgents);

            /* the assumption is that all the registered agents are currently executing TD test or they've completed
             * the execution of the current phase
             */
            if (driver.getConfiguration() != null) {
                LOG.info("TD tests are already executed => mark agents as active TD runners");
                agentsRunningTD.forEach(agent -> driver.getDistributedPhaseMonitor().registerAgentRunningTD(agent));
            }

            if (!idleAgents.isEmpty()) {
                LOG.info("New master must redistribute the work between the agents because of idle agents " +
                        idleAgents.toString());
                idleAgents.forEach(idleAgent ->
                        driver.getTaskBalancer().scheduleWorkRedistributionProcess(driver.getDistributedPhaseMonitor(),
                                agentsRunningTD, driver.getConfiguration(),
                                driver.getDriverState().getDriverConfig().getDistributedConfig(), idleAgent, true));
            }

            // TODO: modify what happens when an agent is announcing his phase completion => we should keep a separate
            //  list with all the agents in this state and consider the phase completed only when both lists
            //  (activeTDRunners and agentsWhichCompletedThe phase) have the same content. (this should help us avoid
            //  the case in which we add an agent that completed the phase again into the list of active TD runners

            // schedule heartbeat task for periodically checking agents
            LOG.info("Scheduling heartbeat task for monitoring agents...");
            driver.scheduleHeartbeatTask();

            // resume execution
            driver.getExecutorService().submit(driver::resumeExecution);
        } else {
            LOG.info("Running as SLAVE");

            // running as slave
            driver.getDriverState().setCurrentState(DriverState.State.SLAVE);

            // schedule heartbeat task to periodically monitor the agents
            driver.scheduleMasterHeartbeatTask();
        }

        driver.getDriverState().getMasterIdLock().readLock().unlock();
    }

    private void processUpdatesFromDriver(Driver currentDriver, String yamlUpdates) {
        LOG.info("Current driver " + currentDriver.getDriverState().getId() + " is processing updates:\n" + yamlUpdates);
        ObjectMapper objectMapper = new ObjectMapper();
        DriverUpdateInfo updates = null;

        try {
            updates = objectMapper.readValue(yamlUpdates, DriverUpdateInfo.class);
        } catch (IOException e) {
            LOG.info("Unable to process updates about the cluster state. Driver will restart now...");
            System.exit(-1);
        }

        currentDriver.getDriverState().updateDriverState(updates, currentDriver)
        ;
        // TODO : treat agents running TD & agents which finished executing the current phase

        // set master if updates were received from the current master running in the cluster
        if (updates.getSourceState() == DriverState.State.MASTER) {
            LOG.info("Received instructions that " + currentDriver.getDriverState().getPathForId(updates.getDriverId()) + "is the " +
                    "current master running in the cluster.");
            currentDriver.getDriverState().getMasterIdLock().writeLock().lock();
            currentDriver.getDriverState().setMasterId(updates.getDriverId());
            currentDriver.getDriverState().getMasterIdLock().writeLock().unlock();
        }
    }

    private void collectUpdatesFromAllDrivers(Driver currentDriver) {
        HttpUtils httpUtils = new HttpUtils();
        List<Integer> ids = IntStream.rangeClosed(0, nrDrivers - 1).boxed().collect(Collectors.toList());

        // send request to all drivers, except the current one
        List<String> paths = ids.stream()
                .filter(id -> id != currentDriver.getDriverState().getId())
                .map(id -> currentDriver.getDriverState().getPathForId(id))
                .map(Driver::getAskForUpdatesPath)
                .collect(Collectors.toList());

        for (String URI : paths) {
            LOG.info("USED URI " + URI);
            HttpResponse driverResponse = httpUtils.sendHttpRequest(HttpUtils.GET_METHOD, "", URI,
                    HttpUtils.HTTP_REQUEST_RETRIES);
            if (driverResponse != null) {
                try {
                    String yamlUpdates = EntityUtils.toString(driverResponse.getEntity());
                    LOG.info("Received updates: " + yamlUpdates);

                    processUpdatesFromDriver(currentDriver, yamlUpdates);

                    // if updates were received from the master -> finish process
                    if (currentDriver.getDriverState().getMasterId() != -1) {
                        break;
                    }
                } catch (IOException e) {
                    LOG.info("Unable to process updates about the cluster state. Driver will restart now...");
                    System.exit(-1);
                }
            }
        }
    }

}

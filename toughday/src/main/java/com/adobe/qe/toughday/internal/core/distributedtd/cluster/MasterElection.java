package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
        } else {
            driverState.getMasterIdLock().readLock().unlock();
        }

        // update driver status
        if (driverState.getMasterId() == driverState.getId()) {
            LOG.info("Running as MASTER");
            driverState.setCurrentState(DriverState.State.MASTER);
        } else {
            LOG.info("Running as SLAVE");
            driverState.setCurrentState(DriverState.State.SLAVE);
        }
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

    private void after(Driver driver) {
        // cancel heartbeat task for the diver elected as the new master
        driver.getDriverState().getMasterIdLock().readLock().lock();
        if (driver.getDriverState().getMasterId() == driver.getDriverState().getId()) {
            driver.getDriverState().setCurrentState(DriverState.State.MASTER);
            LOG.info("Stopping master heartbeat periodic task since this driver was elected as master.");
            driver.cancelMasterHeartBeatTask();
        }
        driver.getDriverState().getMasterIdLock().readLock().unlock();
    }

    private void processUpdatesFromDriver(Driver currentDriver, String yamlUpdates) {
        LOG.info("Current driver " + currentDriver.getDriverState().getId() + " is processing updates.");
        ObjectMapper objectMapper = new ObjectMapper();
        DriverUpdateInfo updates = null;

        try {
            updates = objectMapper.readValue(yamlUpdates, DriverUpdateInfo.class);
        } catch (IOException e) {
            LOG.info("Unable to process updates about the cluster state. Driver will restart now...");
            System.exit(-1);
        }

        // excludes candidates which are not allowed to become master
        updates.getInvalidCandidates().forEach(this::markCandidateAsInvalid);

        // add all agents
        updates.getRegisteredAgents().forEach(currentDriver.getDriverState()::registerAgent);
        LOG.info("After processing update instructions, registered agents is " +
                currentDriver.getDriverState().getRegisteredAgents().toString());
        LOG.info("After processing update instructions, invalid candidates are: " + getInvalidCandidates().toString());

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

    public void collectUpdatesFromAllDrivers(Driver currentDriver) {
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

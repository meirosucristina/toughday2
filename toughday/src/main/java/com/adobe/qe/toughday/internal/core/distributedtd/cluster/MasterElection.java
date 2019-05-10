package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
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
        this.candidates = IntStream.rangeClosed(0, nrDrivers).boxed()
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
    }

    public void markCandidateAsInvalid(int candidateId) {
        this.candidates.remove(candidateId);
    }

    public boolean isCandidateInvalid(int candidateId) {
        return !this.candidates.contains(candidateId);
    }

    public Queue<Integer> getInvalidCandidates() {
        Queue<Integer> invalidCandidates = IntStream.rangeClosed(0, nrDrivers).boxed()
                .collect(Collectors.toCollection(LinkedList::new));
        invalidCandidates.removeAll(this.candidates);

        return invalidCandidates;
    }

    private void before() {
        // restore all valid candidates in case there is no option;
        if (candidates.isEmpty()) {
            LOG.info("Resetting list of candidates to be considered for master election");
            this.candidates = IntStream.rangeClosed(0, nrDrivers).boxed().collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
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
            LOG.info("Stopping master heartbeat periodic task since this driver was elected as master");
            driver.cancelMasterHeartBeatTask();
        }
        driver.getDriverState().getMasterIdLock().readLock().unlock();
    }

    public void collectUpdatesFromAllDrivers(Driver currentDriver) {
        // TODO: send request to all drivers, except the current one
        // TODO: merge responses from all the drivers
        // TODO: set master if updates were received from the current master running in the cluster

    }

}

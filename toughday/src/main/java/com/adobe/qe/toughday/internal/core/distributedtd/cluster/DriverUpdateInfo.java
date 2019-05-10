package com.adobe.qe.toughday.internal.core.distributedtd.cluster;

import java.util.Queue;

public class DriverUpdateInfo {
    private String sourceIP;
    private DriverState.State sourceState;
    private Queue<Integer> invalidCandidates;
    private Queue<String> agents;

    // TODO: add agents currently running tasks
    // TODO: add agents which finished running the current phase


    // dummy constructor used to dump the class
    public DriverUpdateInfo() {}

    public DriverUpdateInfo(String sourceIp, DriverState.State sourceState, Queue<Integer> invalidCandidates, Queue<String> agents) {
        this.sourceIP = sourceIp;
        this.sourceState = sourceState;
        this.invalidCandidates = invalidCandidates;
        this.agents = agents;
    }

    public void setSourceIP(String sourceIP) {
        this.sourceIP = sourceIP;
    }

    public String getSourceIP() {
        return this.sourceIP;
    }

    public void setSourceState(DriverState.State driverState) {
        this.sourceState = driverState;
    }

    public DriverState.State getSourceState() {
        return this.sourceState;
    }

    public void setInvalidCandidates(Queue<Integer> invalidCandidates) {
        this.invalidCandidates = invalidCandidates;
    }

    public Queue<Integer> getInvalidCandidates() {
        return this.invalidCandidates;
    }

    public void setAgents(Queue<String> agents) {
        this.agents = agents;
    }

    public Queue<String> getAgents() {
        return this.agents;
    }


}

package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver;

import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Queue;

public class DriverUpdateInfo {
    private int driverId;
    private DriverState.State sourceState;
    private Queue<Integer> invalidCandidates;
    private Queue<String> registeredAgents;
    private String yamlConfig;
    private String currentPhaseName;
    protected static final Logger LOG = LogManager.getLogger(Engine.class);


    // TODO: add registeredAgents currently running tasks
    // TODO: add registeredAgents which finished running the current phase


    // dummy constructor used to dump the class
    public DriverUpdateInfo() {}

    public DriverUpdateInfo(int driverId, DriverState.State sourceState, Queue<Integer> invalidCandidates,
                            Queue<String> registeredAgents, String yamlConfig, String currentPhaseName) {
        this.driverId = driverId;
        this.sourceState = sourceState;
        this.invalidCandidates = invalidCandidates;
        this.registeredAgents = registeredAgents;
        this.yamlConfig = yamlConfig;
        this.currentPhaseName = currentPhaseName;
    }

    public int getDriverId() {
        return this.driverId;
    }

    public void setDriverId(int driverId) {
        this.driverId = driverId;
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

    public void setRegisteredAgents(Queue<String> registeredAgents) {
        this.registeredAgents = registeredAgents;
    }

    public Queue<String> getRegisteredAgents() {
        return this.registeredAgents;
    }

    public void setYamlConfig(String yamlConfig) {
        this.yamlConfig = yamlConfig;
    }

    public String getYamlConfig() {
        return this.yamlConfig;
    }

    public String getCurrentPhaseName() {
        return this.currentPhaseName;
    }

    public void setCurrentPhaseName(String currentPhaseName) {
        this.currentPhaseName = currentPhaseName;
    }

}

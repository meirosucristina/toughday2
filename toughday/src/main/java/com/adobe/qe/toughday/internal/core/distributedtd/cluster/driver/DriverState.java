package com.adobe.qe.toughday.internal.core.distributedtd.cluster.driver;

import com.adobe.qe.toughday.internal.core.config.Configuration;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import com.adobe.qe.toughday.internal.core.engine.Phase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

public class DriverState {
    // Documentation says that each service has the following DNS A record: my-svc.my-namespace.svc.cluster.local
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final String hostname;
    private static final String SVC_NAME = "driver";
    private final Queue<String> agents = new ConcurrentLinkedQueue<>();
    private final Queue<Integer> inactiveDrivers = new ConcurrentLinkedQueue<>();
    private final int id;
    private int masterId = -1;
    private int nrDrivers;
    private Configuration driverConfig;
    private State currentState;
    private final ReadWriteLock masterIdLock = new ReentrantReadWriteLock();

    public enum State {
        CHOOSING_MASTER,
        PICKED_MASTER,
        SLAVE,
        MASTER
    }

    public String getPathForId(int id) {
        return SVC_NAME + "-" + id + "." + SVC_NAME + "." +
                this.driverConfig.getDistributedConfig().getClusterNamespace() + ".svc.cluster.local";
    }

    public ReadWriteLock getMasterIdLock() {
        return this.masterIdLock;
    }

    public DriverState(String hostname, Configuration driverConfig) {
        this.driverConfig = driverConfig;
        this.hostname = hostname;
        LOG.info("Hostname is " + this.hostname);
        String name_pattern = "driver-[0-9]+\\." + SVC_NAME + "\\." + this.driverConfig.getDistributedConfig().getClusterNamespace() +
                "\\.svc\\.cluster\\.local";

        if (!Pattern.matches(name_pattern, hostname)) {
            throw new IllegalStateException("Driver's name should respect the following format: driver-<id>.<svc_name>.<namespace>.svc.cluster.local");
        }

        String podName = hostname.substring(0, hostname.indexOf("." + SVC_NAME + "." +
                this.driverConfig.getDistributedConfig().getClusterNamespace()));
        this.id = Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1));
        this.nrDrivers = Integer.parseInt(System.getenv("NR_DRIVERS"));

        LOG.info("Nr of drivers running in the cluster: " + this.nrDrivers);
        LOG.info("ID is " + this.id);
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getId() {
        return this.id;
    }

    public int getMasterId() {
        return this.masterId;
    }

    public int getNrDrivers() {
        return this.nrDrivers;
    }

    public Queue<String> getRegisteredAgents() {
        return this.agents;
    }

    public void registerAgent(String agentIdentifier) {
        this.agents.add(agentIdentifier);
    }

    public void addInactiveDriver(int driverIdentifier) {
        this.inactiveDrivers.add(driverIdentifier);
    }

    public Queue<Integer> getInactiveDrivers() {
        return this.inactiveDrivers;
    }

    public void removeInactiveDriver(int driverIdentifier) {
        this.inactiveDrivers.remove(driverIdentifier);
    }

    public void removeAgent(String agentIdentifier) {
        this.agents.remove(agentIdentifier);
    }

    public Configuration getDriverConfig() {
        return this.driverConfig;
    }

    public State getCurrentState() {
        return this.currentState;
    }

    public void setCurrentState(State state) {
        this.currentState = state;
    }

    public void setMasterId(int id) {
        this.masterId = id;
    }

    public void updateDriverState(DriverUpdateInfo updates, Driver currentDriver) {
        // excludes candidates which are not allowed to become master
        updates.getInvalidCandidates().forEach(currentDriver.getMasterElection()::markCandidateAsInvalid);

        // add all agents
        updates.getRegisteredAgents().forEach(currentDriver.getDriverState()::registerAgent);
        LOG.info("After processing update instructions, registered agents is " +
                currentDriver.getDriverState().getRegisteredAgents().toString());
        LOG.info("After processing update instructions, invalid candidates are: " +
                updates.getInvalidCandidates().toString());

        // build TD configuration to be executed distributed
        if (!updates.getYamlConfig().isEmpty() && currentDriver.getConfiguration() == null) {
            LOG.info("Building configuration received from the other drivers running in the cluster.");
            try {
                Configuration configuration = new Configuration(updates.getYamlConfig());
                currentDriver.setConfiguration(configuration);
            } catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IOException |
                    IllegalAccessException e) {
                /* if driver can't build the configuration executed distributed it will not be able to become the master
                 * and to coordinate the entire execution
                 */
                LOG.warn("Exception occurred when building ToughDay configuration to be executed distributed. Driver" +
                        " will leave the cluster");
                System.exit(-1);
            }

            // delete phases that were previously executed
            List<Phase> phases = currentDriver.getConfiguration().getPhases();
            List<Phase> previouslyExecutedPhases = new ArrayList<>();
            for (Phase phase : phases) {
                if (phase.getName().equals(updates.getCurrentPhaseName())) {
                    break;
                }

                previouslyExecutedPhases.add(phase);
            }
            phases.removeAll(previouslyExecutedPhases);

            // set current phase to be monitored
            currentDriver.getDistributedPhaseMonitor().setPhase(phases.get(0));
            LOG.info("Current phase being executed is " + currentDriver.getDistributedPhaseMonitor().getPhase().getName());
        }
    }
}
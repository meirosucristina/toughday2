package com.adobe.qe.toughday.internal.core.distributedtd.tasks;

import com.adobe.qe.toughday.internal.core.distributedtd.HttpUtils;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.Driver;
import com.adobe.qe.toughday.internal.core.distributedtd.cluster.DriverState;
import com.adobe.qe.toughday.internal.core.engine.Engine;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MasterHeartbeatTask implements Runnable {
    protected static final Logger LOG = LogManager.getLogger(Engine.class);

    private final Driver driver;
    private final HttpUtils httpUtils = new HttpUtils();

    public MasterHeartbeatTask(Driver driver) {
        this.driver = driver;
    }

    private void announceDriversThatMasterDied() {
        LOG.info("Starting to announce drivers that the current master just died.");

        for (int i = 0; i < this.driver.getDriverState().getNrDrivers(); i++) {
            // skip current driver and the former master(which just died)
            if (i == this.driver.getDriverState().getId() || i == this.driver.getDriverState().getMasterId()) {
                continue;
            }

            String hostname = this.driver.getDriverState().getPathForId(i);
            HttpResponse driverResponse = this.httpUtils.sendHttpRequest(HttpUtils.POST_METHOD,
                    String.valueOf(this.driver.getDriverState().getMasterId()), Driver.getMasterElectionPath(hostname, HttpUtils.SPARK_PORT),
                    HttpUtils.HTTP_REQUEST_RETRIES);

            if (driverResponse == null) {
                /* the assumption is that the driver failed to respond to heartbeat request and he will receive the
                 * updates after rejoining the cluster.
                 */
                LOG.warn("Failed to announce driver " + hostname + " that the master failed.");
            }

            LOG.info("Successfully announced driver " + hostname + " that the current master died.");
        }

    }

    @Override
    public void run() {
        DriverState driverState = this.driver.getDriverState();

        LOG.info(driverState.getHostname() + ": sending heartbeat message to master: " +
                Driver.getHealthPath(driverState.getPathForId(driverState.getMasterId())));

        /* acquire read lock to prevent modification of the current master when receiving information from a different
        * driver while the current one is informing others to trigger the master election process.
        * */

        this.driver.getDriverState().getMasterIdLock().readLock().lock();
        HttpResponse driverResponse = this.httpUtils.sendHttpRequest(HttpUtils.GET_METHOD, "",
                Driver.getHealthPath(driverState.getPathForId(driverState.getMasterId())),
                HttpUtils.HTTP_REQUEST_RETRIES);

        if (driverResponse == null) {
            LOG.info(driverState.getHostname() + ": master failed to respond to heartbeat message. Master " +
                    "election process will be triggered soon.");
            /* mark candidate as inactive */
            this.driver.getMasterElection().markCandidateAsInvalid(driverState.getMasterId());

            /* announce all drivers that the current master died */
            announceDriversThatMasterDied();

            this.driver.getDriverState().getMasterIdLock().readLock().unlock();

            /* elect a new master */
            this.driver.getMasterElection().electMaster(driver);
        } else {
            // release read lock
            this.driver.getDriverState().getMasterIdLock().readLock().unlock();
        }
    }
}
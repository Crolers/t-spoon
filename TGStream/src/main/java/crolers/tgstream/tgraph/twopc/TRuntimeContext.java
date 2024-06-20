package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.common.Address;
import crolers.tgstream.runtime.NetUtils;
import crolers.tgstream.tgraph.IsolationLevel;
import crolers.tgstream.tgraph.Strategy;
import crolers.tgstream.tgraph.durability.*;
import crolers.tgstream.common.TimestampGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

/**
 * <p>
 * The TRuntimeContext is passed to the relevant operators passing through the TransactionEnvironment.
 * It is serialized and deserialized by every operator once on the task manager.
 * <p>
 * It is used to obtain singleton instances of AbstractOpenOperatorTransactionCloser and
 * AbstractStateOperatorTransactionCloser.
 * <p>
 * There is no need to return singleton instances of AbstractCloseOperatorTransactionCloser, because we want the
 * maximum degree of parallelism for closing transactions.
 */
public class TRuntimeContext implements Serializable {
    private static AbstractOpenOperatorTransactionCloser[] openOperatorTransactionCloserPool;
    private static AbstractStateOperatorTransactionCloser[] stateOperatorTransactionCloserPool;
    private static LocalWALServer localWALServer;

    private AbstractTwoPCParticipant.SubscriptionMode subscriptionMode = AbstractTwoPCParticipant.SubscriptionMode.GENERIC;
    public boolean durable, useDependencyTracking;
    public IsolationLevel isolationLevel;
    public Strategy strategy;
    public int openServerPoolSize = 1, stateServerPoolSize = 1, queryServerPoolSize = 1;
    private boolean synchronous;
    private boolean baselineMode;
    private String[] taskManagers;
    private int tGraphId;
    private int closeBatchSize;
    private int recoverySimulationRate;
    private int numberOfSources;

    public TRuntimeContext(int tGraphId) {
        this.tGraphId = tGraphId;
    }

    public int getGraphId() {
        return tGraphId;
    }

    public void setDurabilityEnabled(boolean durable) {
        this.durable = durable;
    }

    public boolean isDurabilityEnabled() {
        return durable;
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public boolean isDependencyTrackingEnabled() {
        return useDependencyTracking;
    }

    public void setUseDependencyTracking(boolean useDependencyTracking) {
        this.useDependencyTracking = useDependencyTracking;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setSubscriptionMode(AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        this.subscriptionMode = subscriptionMode;
    }

    public AbstractTwoPCParticipant.SubscriptionMode getSubscriptionMode() {
        return subscriptionMode;
    }

    public <T> TransactionsIndex<T> getTransactionsIndex(
            long startTid, TimestampGenerator timestampGenerator) {
        if (getStrategy() == Strategy.OPTIMISTIC && getIsolationLevel() == IsolationLevel.PL4) {
            return new TidForWatermarkingTransactionsIndex<>(startTid, timestampGenerator);
        }

        return new StandardTransactionsIndex<>(startTid, timestampGenerator);
    }

    public void setOpenServerPoolSize(int openServerPoolSize) {
        Preconditions.checkArgument(openServerPoolSize > 0,
                "Server pool size must be greater than 0");
        this.openServerPoolSize = openServerPoolSize;
    }

    public void setStateServerPoolSize(int stateServerPoolSize) {
        Preconditions.checkArgument(stateServerPoolSize > 0,
                "Server pool size must be greater than 0");
        this.stateServerPoolSize = stateServerPoolSize;
    }

    public void setQueryServerPoolSize(int queryServerPoolSize) {
        Preconditions.checkArgument(queryServerPoolSize > 0,
                "Server pool size must be greater than 0");
        this.queryServerPoolSize = queryServerPoolSize;
    }

    public void setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
    }

    public boolean isSynchronous() {
        return synchronous;
    }

    public void setBaselineMode(boolean baselineMode) {
        this.baselineMode = baselineMode;
    }

    public boolean isBaselineMode() {
        return baselineMode;
    }

    public boolean needWaitOnRead() {
        return !isSynchronous() && getIsolationLevel().gte(IsolationLevel.PL3);
    }

    public void setTaskManagers(String[] taskManagers) {
        this.taskManagers = taskManagers;
    }

    public String[] getTaskManagers() {
        return taskManagers;
    }

    public int getCloseBatchSize() {
        return closeBatchSize;
    }

    public void setCloseBatchSize(int closeBatchSize) {
        this.closeBatchSize = closeBatchSize;
    }

    public void setRecoverySimulationRate(int recoverySimulationRate) {
        this.recoverySimulationRate = recoverySimulationRate;
    }

    public int getRecoverySimulationRate() {
        return recoverySimulationRate;
    }

    public void setNumberOfSources(int numberOfSources) {
        this.numberOfSources = numberOfSources;
    }

    public int getNumberOfSources() {
        return numberOfSources;
    }

    // ---------------------- These methods are called upon deserialization

    public AbstractOpenOperatorTransactionCloser getSourceTransactionCloser(int taskNumber) {
        Preconditions.checkArgument(taskNumber >= 0);

        int index = taskNumber % openServerPoolSize;

        synchronized (TRuntimeContext.class) {
            if (openOperatorTransactionCloserPool == null) {
                openOperatorTransactionCloserPool = new AbstractOpenOperatorTransactionCloser[openServerPoolSize];
            }

            if (openOperatorTransactionCloserPool[index] == null) {
                if (isSynchronous()) {
                    openOperatorTransactionCloserPool[index] = new SynchronousOpenOperatorTransactionCloser(subscriptionMode);
                } else {
                    openOperatorTransactionCloserPool[index] = new AsynchronousOpenOperatorTransactionCloser(subscriptionMode);
                }
            }

            return openOperatorTransactionCloserPool[index];
        }
    }


    public AbstractStateOperatorTransactionCloser getAtStateTransactionCloser(int taskNumber) {
        Preconditions.checkArgument(taskNumber >= 0);

        int index = taskNumber % stateServerPoolSize;

        synchronized (TRuntimeContext.class) {
            if (stateOperatorTransactionCloserPool == null) {
                stateOperatorTransactionCloserPool = new AbstractStateOperatorTransactionCloser[stateServerPoolSize];
            }

            if (stateOperatorTransactionCloserPool[index] == null) {
                if (isSynchronous()) {
                    stateOperatorTransactionCloserPool[index] = new SynchronousStateTransactionCloser(subscriptionMode);
                } else {
                    stateOperatorTransactionCloserPool[index] = new AsynchronousStateTransactionCloser(subscriptionMode);
                }
            }

            return stateOperatorTransactionCloserPool[index];
        }
    }

    public LocalWALServer getLocalWALServer(ParameterTool parameterTool) throws IOException {
        Address proxyWALServerAddress = NetUtils.getProxyWALServerAddress(parameterTool);
        synchronized (TRuntimeContext.class) {
            if (localWALServer == null) {
                localWALServer = new LocalWALServer(proxyWALServerAddress.ip, proxyWALServerAddress.port);
            }

            return localWALServer;
        }
    }

    public void startLocalWALServer() throws IOException {
        synchronized (TRuntimeContext.class) {
            if (localWALServer == null) {
                throw new IllegalStateException("Cannot start LocalWALServer if not created!");

            }

            if (localWALServer.isBound()) {
                return;
            }

            NetUtils.getServer(NetUtils.ServerType.WAL, localWALServer);
        }
    }

    public WALService getWALClient() throws IOException {
        if (isDurabilityEnabled()) {
            return WALClient.get(this);
        }

        return new FakeWALClient();
    }

    public SnapshotService getSnapshotService(ParameterTool params) throws IOException {
        if (isDurabilityEnabled()) {
            return SnapshotClient.get(params);
        }

        return new NoSnap();
    }

    /**
     * NOTE: Use it for testing only
     */
    void resetTransactionClosers() {
        stateOperatorTransactionCloserPool = null;
        openOperatorTransactionCloserPool = null;
    }

    // no singleton
    public AbstractCloseOperatorTransactionCloser getSinkTransactionCloser() {
        if (isSynchronous()) {
            return new SynchronousSinkTransactionCloser(getCloseBatchSize());
        }

        return new AsynchronousSinkTransactionCloser(getCloseBatchSize());
    }
}

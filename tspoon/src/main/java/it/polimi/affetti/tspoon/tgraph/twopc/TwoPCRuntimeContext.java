package it.polimi.affetti.tspoon.tgraph.twopc;

import java.io.Serializable;

/**
 * Created by affo on 04/12/17.
 * <p>
 * The TwoPCRuntimeContext is passed to the relevant operators passing through the TransactionEnvironment.
 * It is serialized and deserialized by every operator once on the task manager.
 * <p>
 * It is used to obtain singleton instances of AbstractOpenOperatorTransactionCloser and
 * AbstractStateOperatorTransactionCloser.
 * <p>
 * There is no need to return singleton instances of CloseSinkTransactionCloser, because we want the
 * maximum degree of parallelism for closing transactions.
 */
public class TwoPCRuntimeContext implements Serializable {
    private static AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;
    private static AbstractStateOperatorTransactionCloser stateOperatorTransactionCloser;
    public boolean durable;

    public void setDurabilityEnabled(boolean durable) {
        this.durable = durable;
    }

    public boolean isDurabilityEnabled() {
        return durable;
    }

    // ---------------------- These methods are called upon deserialization

    public AbstractOpenOperatorTransactionCloser getSourceTransactionCloser() {
        synchronized (TwoPCRuntimeContext.class) {
            if (openOperatorTransactionCloser == null) {
                if (isDurabilityEnabled()) {
                    openOperatorTransactionCloser = new DurableOpenOperatorTransactionCloser();
                } else {
                    openOperatorTransactionCloser = new VolatileOpenOperatorTransactionCloser();
                }
            }

            return openOperatorTransactionCloser;
        }
    }

    public AbstractStateOperatorTransactionCloser getAtStateTransactionCloser() {
        synchronized (TwoPCRuntimeContext.class) {
            if (stateOperatorTransactionCloser == null) {
                if (isDurabilityEnabled()) {
                    stateOperatorTransactionCloser = new DurableStateTransactionCloser();
                } else {
                    stateOperatorTransactionCloser = new VolatileStateTransactionCloser();
                }
            }

            return stateOperatorTransactionCloser;
        }
    }

    // no singleton
    public CloseSinkTransactionCloser getSinkTransactionCloser() {
        if (isDurabilityEnabled()) {
            return new DurableSinkTransactionCloser();
        }

        return new VolatileSinkTransactionCloser();
    }
}

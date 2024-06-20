package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;
import crolers.tgstream.tgraph.db.Transaction;

/**
 *
 * Strategy for updating the dependencyTracking of a particular Transaction.
 */
public interface DependencyTrackingStrategy {
    /**
     * Updates the dependencies of transaction `transaction` by using the methods
     * Transaction::addDependency/ies
     *
     * @param transaction the transaction that needs an update for dependencies
     * @param object the object that is updated by the current operation
     * @param version the version read by this operation
     * @param createdBy the tid that created the version updated by this operation
     */
    <T> void updateDependencies(Transaction<T> transaction, Object<T> object, long version, long createdBy);
}

package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.Vote;
import crolers.tgstream.tgraph.db.Object;
import crolers.tgstream.tgraph.db.Transaction;

/**
 * Created by crolers
 */
public class StandardDependencyTrackingStrategy implements DependencyTrackingStrategy {
    @Override
    public <T> void updateDependencies(Transaction<T> transaction, Object<T> object, long version, long createdBy) {
        if (transaction.vote == Vote.REPLAY) {
            transaction.addDependency(object.getYoungestAccessorTidBefore(transaction.tid));
        }
    }
}

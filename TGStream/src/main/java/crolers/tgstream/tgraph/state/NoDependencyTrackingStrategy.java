package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;
import crolers.tgstream.tgraph.db.Transaction;

public class NoDependencyTrackingStrategy implements DependencyTrackingStrategy {
    @Override
    public <T> void updateDependencies(Transaction<T> transaction, Object<T> object, long version, long createdBy) {
        // does nothing
    }
}

package crolers.tgstream.tgraph.backed;

import crolers.tgstream.evaluation.EvalConfig;
import crolers.tgstream.evaluation.TunableSource;
import crolers.tgstream.tgraph.query.Query;
import crolers.tgstream.tgraph.query.QueryID;
import crolers.tgstream.tgraph.query.QuerySupplier;
import crolers.tgstream.tgraph.query.RandomQuerySupplier;

public class TunableQuerySource extends TunableSource<Query> {
    private transient QuerySupplier supplier;
    private final int keyspaceSize, averageQuerySize;
    private final String namespace;
    private int stdDevQuerySize;

    public TunableQuerySource(EvalConfig config, String trackingServerNameForDiscovery,
                              String namespace, int keyspaceSize, int averageQuerySize, int stdDevQuerySize) {
        super(config, trackingServerNameForDiscovery);
        this.keyspaceSize = keyspaceSize;
        this.averageQuerySize = averageQuerySize;
        this.stdDevQuerySize = stdDevQuerySize;
        this.namespace = namespace;
    }

    @Override
    protected Query getNext(int count) {
        if (supplier == null) {
            supplier = new RandomQuerySupplier(
                    namespace, taskNumber, Transfer.KEY_PREFIX, keyspaceSize, averageQuerySize, stdDevQuerySize);
        }

        return supplier.getQuery(new QueryID(taskNumber, (long) count));
    }
}
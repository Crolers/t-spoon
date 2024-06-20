package crolers.tgstream.evaluation;

import crolers.tgstream.tgraph.query.QueryID;
import crolers.tgstream.tgraph.query.QuerySupplier;
import crolers.tgstream.tgraph.query.Query;

public class EvaluationQuerySource extends TunableSource<Query> {
    private final QuerySupplier supplier;

    public EvaluationQuerySource(EvalConfig config, String trackingServerNameForDiscovery,
            QuerySupplier querySupplier) {
        super(config, trackingServerNameForDiscovery);
        this.supplier = querySupplier;
    }

    @Override
    protected Query getNext(int count) {
        return supplier.getQuery(new QueryID(taskNumber, (long) count));
    }
}

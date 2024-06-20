package crolers.tgstream.tgraph.query;

import crolers.tgstream.common.ControlledSource;

/**
 * Created by crolers
 */
// up to now, we provide only single-query MultiStateQueries
public class QuerySource extends ControlledSource<MultiStateQuery> {
    private QuerySupplier querySupplier = new NullQuerySupplier();
    private long count = 0;

    public void setQuerySupplier(QuerySupplier querySupplier) {
        this.querySupplier = querySupplier;
    }

    @Override
    public void run(SourceContext<MultiStateQuery> sourceContext) throws Exception {
        MultiStateQuery multiStateQuery = new MultiStateQuery();
        Query query;
        do {
            query = querySupplier.getQuery(new QueryID(taskId, count));
            if (query != null) {
                multiStateQuery.addQuery(query);
                sourceContext.collect(multiStateQuery);
                count++;
            }
            multiStateQuery.queries.clear();
        } while (!stop && query != null);

        waitForFinish();
    }

    @Override
    public void onJobFinish() {
        super.onJobFinish();
        stop = true;
    }
}

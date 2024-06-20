package crolers.tgstream.tgraph.query;

public class LimitQuerySupplier implements QuerySupplier {
    private QuerySupplier qs;
    private int limit, count = 0;

    public LimitQuerySupplier(QuerySupplier qs, int limit) {
        this.qs = qs;
        this.limit = limit;
    }

    @Override
    public Query getQuery(QueryID queryID) {
        Query q = null;
        if (count < limit) {
            q = qs.getQuery(queryID);
            count++;
        }
        return q;
    }
}

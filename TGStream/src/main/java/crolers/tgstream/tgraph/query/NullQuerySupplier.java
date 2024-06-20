package crolers.tgstream.tgraph.query;

/**
 * Created by crolers
 */
public class NullQuerySupplier implements QuerySupplier {
    @Override
    public Query getQuery(QueryID queryID) {
        return null;
    }
}

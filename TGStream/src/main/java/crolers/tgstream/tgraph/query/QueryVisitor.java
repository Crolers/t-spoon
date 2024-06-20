package crolers.tgstream.tgraph.query;

/**
 * NOTE: visits must populate the QueryResult of the passed Query.
 */
public interface QueryVisitor {
    void visit(Query query);

    <T> void visit(PredicateQuery<T> query);
}

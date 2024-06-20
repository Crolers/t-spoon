package crolers.tgstream.tgraph.query;

import java.io.Serializable;

public interface QuerySupplier extends Serializable {
    Query getQuery(QueryID queryID);
}

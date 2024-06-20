package crolers.tgstream.tgraph.query;

import crolers.tgstream.evaluation.UniquelyRepresentableForTracking;
import org.apache.flink.api.java.tuple.Tuple2;

public class QueryID extends Tuple2<Integer, Long> implements UniquelyRepresentableForTracking {
    public QueryID() {
    }

    public QueryID(Integer taskID, Long incrementalID) {
        super(taskID, incrementalID);
    }

    @Override
    public String toString() {
        return f0 + "." + f1;
    }

    @Override
    public String getUniqueRepresentation() {
        return toString();
    }
}

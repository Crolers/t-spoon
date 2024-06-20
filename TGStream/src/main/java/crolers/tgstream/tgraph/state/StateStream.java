package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.TStream;
import crolers.tgstream.tgraph.TransactionResult;
import crolers.tgstream.tgraph.query.QueryResult;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by crolers
 */
public class StateStream<T> {
    public final TStream<T> leftUnchanged;
    public final DataStream<QueryResult> queryResults;
    public final DataStream<TransactionResult> spuResults;

    public StateStream(TStream<T> leftUnchanged,
                       DataStream<QueryResult> queryResults,
                       DataStream<TransactionResult> spuResults) {
        this.leftUnchanged = leftUnchanged;
        this.queryResults = queryResults;
        this.spuResults = spuResults;
    }
}

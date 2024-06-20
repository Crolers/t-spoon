package crolers.tgstream.tgraph;

import crolers.tgstream.tgraph.query.MultiStateQuery;
import crolers.tgstream.tgraph.query.Query;
import crolers.tgstream.tgraph.state.StateFunction;
import crolers.tgstream.tgraph.state.StateOperator;
import crolers.tgstream.tgraph.state.OptimisticStateOperator;
import crolers.tgstream.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;

/**
 * Created by crolers
 */
public class OTStream<T> extends AbstractTStream<T> {
    public OTStream(DataStream<Enriched<T>> enriched, SplitStream<Query> queryStream, int tGraphID) {
        super(enriched, queryStream, tGraphID);
    }

    public static <T> OpenStream<T> fromStream(
            DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphId) {
        OpenOutputs<T> outputs = AbstractTStream.open(ds, queryStream, tGraphId);
        return new OpenStream<>(
                new OTStream<>(outputs.enrichedDataStream, outputs.queryStream, tGraphId),
                outputs.watermarks);
    }

    @Override
    protected <U> OTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new OTStream<>(newStream, queryStream, tGraphID);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, StateFunction<T, V> stateFunction, KeySelector<T, String> ks) {
        return new OptimisticStateOperator<>(
                tGraphID, nameSpace, stateFunction, ks,
                getTransactionEnvironment().createTransactionalRuntimeContext(tGraphID)
        );
    }
}

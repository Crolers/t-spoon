package crolers.tgstream.tgraph;

import crolers.tgstream.tgraph.functions.Scheduler;
import crolers.tgstream.tgraph.query.MultiStateQuery;
import crolers.tgstream.tgraph.query.Query;
import crolers.tgstream.tgraph.state.PessimisticStateOperator;
import crolers.tgstream.tgraph.state.StateFunction;
import crolers.tgstream.tgraph.state.StateOperator;
import crolers.tgstream.tgraph.state.StateStream;
import crolers.tgstream.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;

public class PTStream<T> extends AbstractTStream<T> {
    private boolean alreadyScheduled = false;

    public PTStream(DataStream<Enriched<T>> enriched, SplitStream<Query> queryStream, int tGraphID) {
        super(enriched, queryStream, tGraphID);
    }


    public static <T> OpenStream<T> fromStream(
            DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphID) {
        OpenOutputs<T> outputs = AbstractTStream.open(ds, queryStream, tGraphID);
        return new OpenStream<>(
                new PTStream<>(outputs.enrichedDataStream, outputs.queryStream, tGraphID),
                outputs.watermarks);
    }

    @Override
    protected <U> PTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new PTStream<>(newStream, queryStream, tGraphID);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, StateFunction<T, V> stateFunction, KeySelector<T, String> ks) {
        return new PessimisticStateOperator<>(
                tGraphID, nameSpace, stateFunction, ks,
                getTransactionEnvironment().createTransactionalRuntimeContext(tGraphID));
    }

    @Override
    public <V> StateStream<T> state(
            String nameSpace,
            KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction,
            int partitioning) {
        dataStream = applySchedulerIfNecessary(dataStream);
        return super.state(nameSpace, ks, stateFunction, partitioning);
    }

    private <U> DataStream<Enriched<U>> applySchedulerIfNecessary(DataStream<Enriched<U>> newStream) {
        IsolationLevel isolationLevel = getTransactionEnvironment().getIsolationLevel();

        if (!alreadyScheduled && isolationLevel == IsolationLevel.PL4) {
            return applyScheduler(newStream);
        }

        return newStream;
    }

    private <U> DataStream<Enriched<U>> applyScheduler(DataStream<Enriched<U>> newStream) {
        alreadyScheduled = true;
        return newStream
                .flatMap(new Scheduler<>())
                .setParallelism(1)
                .name("Scheduler");
    }
}

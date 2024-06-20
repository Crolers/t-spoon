package crolers.tgstream.tgraph;

import crolers.tgstream.tgraph.state.StateFunction;
import crolers.tgstream.common.FlatMapFunction;
import crolers.tgstream.common.TWindowFunction;
import crolers.tgstream.tgraph.state.StateStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface TStream<T> {
    <U> TStream<U> map(MapFunction<T, U> fn);

    <U> TStream<U> flatMap(FlatMapFunction<T, U> flatMapFunction);

    <U> TStream<U> window(TWindowFunction<T, U> windowFunction);

    TStream<T> filter(FilterFunction<T> filterFunction);

    TStream<T> keyBy(KeySelector<T, ?> keySelector);

    <V> StateStream<T> state(
            String nameSpace, KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction, int partitioning);

    DataStream<Enriched<T>> getEnclosingStream();

    int getTGraphID();
}

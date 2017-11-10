package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.TWindowFunction;
import it.polimi.affetti.tspoon.tgraph.functions.FilterWrapper;
import it.polimi.affetti.tspoon.tgraph.functions.FlatMapWrapper;
import it.polimi.affetti.tspoon.tgraph.functions.MapWrapper;
import it.polimi.affetti.tspoon.tgraph.functions.WindowWrapper;
import it.polimi.affetti.tspoon.tgraph.state.*;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OptimisticOpenOperator;
import it.polimi.affetti.tspoon.tgraph.twopc.TwoPCFactory;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Created by affo on 13/07/17.
 */
public class OTStream<T> implements TStream<T> {
    private DataStream<Enriched<T>> ds;
    private final TwoPCFactory factory;

    OTStream(DataStream<Enriched<T>> ds, TwoPCFactory factory) {
        this.ds = ds;
        this.factory = factory;
    }

    private <U> OTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new OTStream<>(newStream, factory);
    }

    public static <T> OpenStream<T> fromStream(DataStream<T> ds, TwoPCFactory factory) {
        // TODO hack: fix lifting type
        TypeInformation<Enriched<T>> type = ds
                .map(new MapFunction<T, Enriched<T>>() {
                    @Override
                    public Enriched<T> map(T t) throws Exception {
                        return null;
                    }
                }).getType();
        OptimisticOpenOperator<T> openOperator = new OptimisticOpenOperator<>(
                factory.getTransactionsIndex(),
                factory.getSourceTransactionCloser());
        SingleOutputStreamOperator<Enriched<T>> enriched = ds
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Integer> watermarks = enriched.getSideOutput(openOperator.watermarkTag);
        DataStream<Tuple2<Long, Vote>> wal = enriched.getSideOutput(openOperator.logTag);
        return new OpenStream<>(new OTStream<>(enriched, factory), watermarks, wal);
    }

    public <U> OTStream<U> map(MapFunction<T, U> fn) {
        return replace(ds.map(new MapWrapper<T, U>() {
            @Override
            public U doMap(T e) throws Exception {
                return fn.map(e);
            }
        }));
    }

    public <U> OTStream<U> flatMap(FlatMapFunction<T, U> flatMapFunction) {
        return replace(ds.flatMap(
                new FlatMapWrapper<T, U>() {
                    @Override
                    protected List<U> doFlatMap(T value) throws Exception {
                        return flatMapFunction.flatMap(value);
                    }
                }
        ));
    }

    @Override
    public <U> TStream<U> window(TWindowFunction<T, U> windowFunction) {
        DataStream<Enriched<U>> windowed = ds.keyBy(
                new KeySelector<Enriched<T>, Integer>() {
                    @Override
                    public Integer getKey(Enriched<T> enriched) throws Exception {
                        return enriched.metadata.timestamp;
                    }
                }).flatMap(new WindowWrapper<T, U>() {
            @Override
            protected U apply(List<T> value) throws Exception {
                return windowFunction.apply(value);
            }
        });
        return replace(windowed);
    }

    public OTStream<T> filter(FilterFunction<T> filterFunction) {
        return replace(ds.map(new FilterWrapper<T>() {
            @Override
            protected boolean doFilter(T value) throws Exception {
                return filterFunction.filter(value);
            }
        }));
    }

    @Override
    public TStream<T> keyBy(KeySelector<T, ?> keySelector) {
        ds = ds.keyBy(new KeySelector<Enriched<T>, Object>() {
            @Override
            public Object getKey(Enriched<T> enriched) throws Exception {
                return keySelector.getKey(enriched.value);
            }
        });
        return this;
    }

    public <V> StateStream<T, V> state(
            String nameSpace, OutputTag<Update<V>> updatesTag, KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction, int partitioning) {
        ds = ds.keyBy(
                new KeySelector<Enriched<T>, String>() {
                    @Override
                    public String getKey(Enriched<T> e) throws Exception {
                        return ks.getKey(e.value);
                    }
                });

        StateOperator<T, V> stateOperator = new OptimisticStateOperator<>(
                nameSpace, stateFunction, updatesTag, factory.getAtStateTransactionCloser());
        SingleOutputStreamOperator<Enriched<T>> mainStream = ds.transform(
                "StateOperator: " + nameSpace, ds.getType(), stateOperator)
                .name(nameSpace).setParallelism(partitioning);

        DataStream<Update<V>> updates = mainStream.getSideOutput(updatesTag);
        return new StateStream<>(replace(mainStream), updates);
    }

    @Override
    public DataStream<Enriched<T>> getEnclosingStream() {
        return ds;
    }
}

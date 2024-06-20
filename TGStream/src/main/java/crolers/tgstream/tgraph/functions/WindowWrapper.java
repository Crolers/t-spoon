package crolers.tgstream.tgraph.functions;

import crolers.tgstream.tgraph.BatchCompletionChecker;
import crolers.tgstream.tgraph.BatchID;
import crolers.tgstream.tgraph.Enriched;
import crolers.tgstream.tgraph.Metadata;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Requires keyBy timestamp
 */
public abstract class WindowWrapper<I, O> extends RichFlatMapFunction<Enriched<I>, Enriched<O>> {
    private Map<Long, List<Enriched<I>>> batches = new HashMap<>();
    private transient BatchCompletionChecker completionChecker;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        completionChecker = new BatchCompletionChecker();
    }

    private List<Enriched<I>> addElement(long timestamp, Enriched<I> element) {
        List<Enriched<I>> batch = batches.computeIfAbsent(timestamp, k -> new LinkedList<>());
        batch.add(element);
        return batch;
    }

    @Override
    public void flatMap(Enriched<I> element, Collector<Enriched<O>> collector) throws Exception {
        long timestamp = element.metadata.timestamp;
        List<Enriched<I>> batch = addElement(timestamp, element);

        boolean complete = completionChecker.checkCompleteness(timestamp, element.metadata.batchID);

        if (complete) {
            batches.remove(timestamp);
            completionChecker.freeIndex(timestamp);

            Stream<Metadata> metas = batch.stream().map(el -> el.metadata);

            Metadata reduced = metas
                    .reduce((accumulator, meta) -> {
                        accumulator.cohorts.addAll(meta.cohorts);
                        accumulator.vote = accumulator.vote.merge(meta.vote);
                        accumulator.dependencyTracking.addAll(meta.dependencyTracking);
                        return accumulator;
                    })
                    .orElseThrow(
                            () -> new RuntimeException("Empty batch in windowing function")); // merge metadata

            // reset the batch id
            Metadata newMetadata = reduced.deepClone(new BatchID(reduced.tid));
            List<I> values = batch.stream().map(el -> el.value).collect(Collectors.toList());
            O result = apply(values);
            collector.collect(Enriched.of(newMetadata, result));
        }
    }

    protected abstract O apply(List<I> value) throws Exception;
}

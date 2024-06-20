package crolers.tgstream.tgraph.functions;

import crolers.tgstream.tgraph.Enriched;
import crolers.tgstream.tgraph.Metadata;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

public abstract class FlatMapWrapper<I, O> implements FlatMapFunction<Enriched<I>, Enriched<O>> {
    @Override
    public void flatMap(Enriched<I> e, Collector<Enriched<O>> collector) throws Exception {
        List<O> outputList = doFlatMap(e.value);

        if (outputList == null || outputList.isEmpty()) {
            return;
        }

        Iterable<Metadata> newMetas = e.metadata.newStep(outputList.size());
        Iterator<Metadata> metadataIterator = newMetas.iterator();
        for (O outElement : outputList) {
            Metadata metadata = metadataIterator.next();
            collector.collect(Enriched.of(metadata, outElement));
        }
    }

    protected abstract List<O> doFlatMap(I value) throws Exception;
}

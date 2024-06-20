package crolers.tgstream.tgraph.functions;

import crolers.tgstream.tgraph.Enriched;
import org.apache.flink.api.common.functions.MapFunction;

public abstract class MapWrapper<I, O> implements MapFunction<Enriched<I>, Enriched<O>> {
    @Override
    public Enriched<O> map(Enriched<I> e) throws Exception {
        return e.replace(doMap(e.value));
    }

    public abstract O doMap(I e) throws Exception;
}

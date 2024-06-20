package crolers.tgstream.tgraph.functions;

import crolers.tgstream.tgraph.Enriched;
import org.apache.flink.api.common.functions.MapFunction;

public abstract class FilterWrapper<T> implements MapFunction<Enriched<T>, Enriched<T>> {
    @Override
    public Enriched<T> map(Enriched<T> e) throws Exception {
        T value = doFilter(e.value) ? e.value : null;
        return e.replace(value);
    }

    protected abstract boolean doFilter(T value) throws Exception;
}

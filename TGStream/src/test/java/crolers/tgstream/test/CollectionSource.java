package crolers.tgstream.test;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;

public class CollectionSource<T> implements SourceFunction<T> {
    Collection<T> elements;

    public CollectionSource(Collection<T> elements) {
        this.elements = elements;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        for (T element : elements) {
            sourceContext.collect(element);
        }
    }

    @Override
    public void cancel() {

    }
}

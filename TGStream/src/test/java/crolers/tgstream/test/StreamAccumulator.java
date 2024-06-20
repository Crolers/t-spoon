package crolers.tgstream.test;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.util.LinkedList;

/**
 * Created by crolers
 */
public class StreamAccumulator<T> implements Accumulator<T, LinkedList<T>> {
    private LinkedList<T> accumulator = new LinkedList<>();

    @Override
    public void add(T t) {
        accumulator.add(t);
    }

    @Override
    public LinkedList<T> getLocalValue() {
        return accumulator;
    }

    @Override
    public void resetLocal() {
        accumulator = new LinkedList<>();
    }

    @Override
    public void merge(Accumulator<T, LinkedList<T>> accumulator) {
        this.accumulator.addAll(accumulator.getLocalValue());
    }

    @Override
    public Accumulator<T, LinkedList<T>> clone() {
        Accumulator<T, LinkedList<T>> copied = new StreamAccumulator<>();
        for (T e : accumulator) {
            copied.add(e);
        }
        return copied;
    }
}

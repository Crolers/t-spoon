package crolers.tgstream.tgraph.db;

import crolers.tgstream.tgraph.state.StateFunction;

public interface ObjectFunction<V> {
    V defaultValue();

    V copyValue(V value);

    boolean invariant(V value);

    StateFunction<?, V> getStateFunction();

    static <T> ObjectFunction<T> fromStateFunction(StateFunction<?, T> stateFunction) {
        return new ObjectFunction<T>() {
            @Override
            public T defaultValue() {
                return stateFunction.defaultValue();
            }

            @Override
            public T copyValue(T value) {
                return stateFunction.copyValue(value);
            }

            @Override
            public boolean invariant(T value) {
                return stateFunction.invariant(value);
            }

            @Override
            public StateFunction<?, T> getStateFunction() {
                return stateFunction;
            }
        };
    }
}

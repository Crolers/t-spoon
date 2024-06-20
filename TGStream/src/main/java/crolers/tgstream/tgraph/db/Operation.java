package crolers.tgstream.tgraph.db;

import crolers.tgstream.tgraph.state.StateFunction;

import java.io.Serializable;
import java.util.function.Consumer;

public interface Operation<V> extends Consumer<ObjectHandler<V>>, Serializable {
    static <T, V> Operation<V> from(T element, StateFunction<T, V> stateFunction) {
        return handler -> stateFunction.apply(element, handler);
    }
}

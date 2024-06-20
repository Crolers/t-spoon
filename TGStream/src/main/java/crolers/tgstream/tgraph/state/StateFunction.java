package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.ObjectHandler;

import java.io.Serializable;

//T 为 function 操作的 元素类型，V 为可查询的状态 value类型
public interface StateFunction<T, V> extends Serializable {
    V defaultValue();

    V copyValue(V value);

    boolean invariant(V value);

    void apply(T element, ObjectHandler<V> handler);
}

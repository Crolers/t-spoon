package crolers.tgstream.common;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.List;

public interface FlatMapFunction<I, O> extends Function, Serializable {
    List<O> flatMap(I input) throws Exception;
}

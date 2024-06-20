package crolers.tgstream.common;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.List;

/**
 * Created by crolers
 */
public interface TWindowFunction<I, O> extends Function, Serializable {
    O apply(List<I> batch);
}

package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.TStream;
import org.apache.flink.streaming.api.datastream.DataStream;

public class OpenStream<T> {
    public final TStream<T> opened;
    public final DataStream<Long> watermarks;

    public OpenStream(
            TStream<T> opened,
            DataStream<Long> watermarks) {
        this.opened = opened;
        this.watermarks = watermarks;
    }
}

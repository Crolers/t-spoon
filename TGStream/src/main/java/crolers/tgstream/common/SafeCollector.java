package crolers.tgstream.common;

import crolers.tgstream.tgraph.Enriched;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

public class SafeCollector<T> {
    private Output<StreamRecord<Enriched<T>>> out;
    private StreamRecord<?> streamRecord;

    public SafeCollector(Output<StreamRecord<Enriched<T>>> out) {
        this.out = out;
        this.streamRecord = new StreamRecord<>(null);
    }

    public synchronized void safeCollect(StreamRecord<Enriched<T>> streamRecord) {
        this.streamRecord = streamRecord;
        out.collect(streamRecord);
    }

    public synchronized void safeCollect(Enriched<T> element) {
        streamRecord.setTimestamp(element.metadata.timestamp);
        out.collect(streamRecord.replace(element));
    }

    public synchronized <U> void safeCollect(OutputTag<U> outputTag, U element) {
        if (streamRecord == null) {
            streamRecord = new StreamRecord<>(element);
        }
        out.collect(outputTag, streamRecord.replace(element));
    }
}

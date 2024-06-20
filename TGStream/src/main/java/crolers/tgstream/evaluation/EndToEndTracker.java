package crolers.tgstream.evaluation;

import crolers.tgstream.common.RecordTracker;
import crolers.tgstream.tgraph.backed.Transfer;
import crolers.tgstream.tgraph.backed.TransferID;
import org.apache.flink.util.OutputTag;

/**
 * Created by crolers
 */
public class EndToEndTracker extends RecordTracker<Transfer, TransferID> {
    public static final String metricName = "end2end";

    public EndToEndTracker(boolean isBegin) {
        super(metricName, isBegin);
    }

    @Override
    public OutputTag<TransferID> createRecordTrackingOutputTag(String label) {
        return new OutputTag<TransferID>(label) {};
    }

    @Override
    protected TransferID extractId(Transfer transfer) {
        return transfer.f0;
    }
}

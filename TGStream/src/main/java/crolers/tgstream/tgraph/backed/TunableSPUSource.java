package crolers.tgstream.tgraph.backed;

import crolers.tgstream.evaluation.TunableSource;
import crolers.tgstream.evaluation.EvalConfig;
import crolers.tgstream.tgraph.state.RandomSPUSupplier;
import crolers.tgstream.tgraph.state.SinglePartitionUpdate;
import crolers.tgstream.tgraph.state.SinglePartitionUpdateID;

import java.util.Random;

public class TunableSPUSource extends TunableSource<SinglePartitionUpdate> {
    private RandomSPUSupplier supplier;
    private Random random;

    public TunableSPUSource(EvalConfig config, String trackingServerNameForDiscovery, RandomSPUSupplier supplier) {
        super(config, trackingServerNameForDiscovery);
        this.supplier = supplier;

    }

    @Override
    protected SinglePartitionUpdate getNext(int count) {
        if (random == null) {
            random = new Random(taskNumber);
        }

        return supplier.next(new SinglePartitionUpdateID(taskNumber, (long) count), random);
    }
}
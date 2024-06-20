package crolers.tgstream.tgraph.backed;

import crolers.tgstream.common.ControlledSource;
import crolers.tgstream.tgraph.state.RandomSPUSupplier;
import crolers.tgstream.tgraph.state.SinglePartitionUpdate;
import crolers.tgstream.tgraph.state.SinglePartitionUpdateID;
import org.apache.flink.configuration.Configuration;

import java.util.Random;

/**
 * Goes as fast as back-pressure allows
 */
public class SPUSource extends ControlledSource<SinglePartitionUpdate> {
    private Random random;
    private RandomSPUSupplier supplier;
    private final String namespace;
    private final int keyspaceSize;

    private int count, limit;
    private final int globalLimit;

    public SPUSource(String namespace, int keyspaceSize, int limit, RandomSPUSupplier supplier) {
        this.namespace = namespace;
        this.keyspaceSize = keyspaceSize;

        this.count = 0;
        this.globalLimit = limit;
        this.supplier = supplier;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.limit = globalLimit / numberOfTasks;

        if (taskId == 0) {
            this.limit += globalLimit % numberOfTasks;
        }

        random = new Random(taskId);
    }

    @Override
    public void run(SourceContext<SinglePartitionUpdate> sourceContext) throws Exception {

        while (count < limit) {
            SinglePartitionUpdate next = supplier.next(new SinglePartitionUpdateID(0, (long) count), random);
            sourceContext.collect(next);
            count++;
        }

        waitForFinish();
    }
}

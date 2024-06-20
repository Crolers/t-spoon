package crolers.tgstream.tgraph.state;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by crolers
 */
public interface RandomSPUSupplier extends Serializable {
    SinglePartitionUpdate next(SinglePartitionUpdateID spuID, Random random);
}

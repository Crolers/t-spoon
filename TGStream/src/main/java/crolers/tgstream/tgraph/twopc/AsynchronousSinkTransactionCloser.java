package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.Metadata;

import java.util.Collections;

/**
 * Created by crolers
 */
public class AsynchronousSinkTransactionCloser extends AbstractCloseOperatorTransactionCloser {
    public AsynchronousSinkTransactionCloser(int closeBatchSize) {
        super(closeBatchSize);
    }

    @Override
    public void applyProtocolOnMetadata(Metadata metadata) throws Exception {
        long dependency;

        if (metadata.dependencyTracking.isEmpty()) {
            dependency = -1;
        } else {
            //dependency：同属一个事务 t 的最近的 element 的 执行时间戳（最近处理的 element 即 执行时间戳最大的）
            dependency = Collections.max(metadata.dependencyTracking);
        }

        String message = CloseTransactionNotification.serialize(
                metadata.tGraphID,
                metadata.timestamp,
                metadata.vote,
                metadata.cohorts.size(),
                dependency
        );

        send(metadata.coordinator, message);
        send(metadata.cohorts, message);
    }
}

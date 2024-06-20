package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.Vote;
import crolers.tgstream.tgraph.db.PessimisticTransactionExecutor;
import crolers.tgstream.tgraph.Enriched;
import crolers.tgstream.tgraph.db.Transaction;
import crolers.tgstream.tgraph.twopc.TRuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.HashSet;

/**
 * Created by crolers
 * <p>
 * NOTE that implementing reads or write locks within a single state operator is useless because requires pre-processing
 * of the transaction itself. When a new transaction comes, if the key is either read or write locked, it waits for it to become
 * free. However we store if a transaction was read-only because this information can be used by the external queries
 * (that are read-only by definition).
 * <p>
 * This implies that, excluding external queries, internal transactions are inherently PL3 or PL4 isolated.
 */
public class PessimisticStateOperator<T, V> extends StateOperator<T, V> {
    private transient PessimisticTransactionExecutor transactionExecutor;

    public PessimisticStateOperator(
            int tGraphID,
            String nameSpace,
            StateFunction<T, V> stateFunction,
            KeySelector<T, String> ks,
            TRuntimeContext tRuntimeContext) {
        super(tGraphID, nameSpace, stateFunction, ks, tRuntimeContext);
    }

    @Override
    public void open() throws Exception {
        super.open();
        transactionExecutor = new PessimisticTransactionExecutor(1);
    }

    @Override
    protected void execute(String key, Enriched<T> record, Transaction<V> transaction) throws Exception {
        transactionExecutor.executeOperation(key, transaction,
                () -> {
                    record.metadata.vote = transaction.vote;
                    record.metadata.dependencyTracking = new HashSet<>(transaction.getDependencies());
                    if (transaction.vote == Vote.COMMIT) {
                        V version = transaction.getVersion(key);
                        // The namespace contains the partition for later recovery
                        record.metadata.addUpdate(shardID, key, version);
                    }
                    collector.safeCollect(record);
                });
    }

    @Override
    protected void onGlobalTermination(Transaction<V> transaction) {
        transactionExecutor.onGlobalTermination(transaction);
    }
}

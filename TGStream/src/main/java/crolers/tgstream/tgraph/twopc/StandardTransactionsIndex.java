package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.common.TimestampGenerator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by crolers
 */
public class StandardTransactionsIndex<T> extends TransactionsIndex<T> {
    private Map<Long, Long> timestampTidMapping = new HashMap<>();
    private Map<Long, Set<Long>> tidTimestampMapping = new HashMap<>();

    public StandardTransactionsIndex(long startingTid, TimestampGenerator timestampGenerator) {
        super(startingTid, timestampGenerator);
    }

    @Override
    public Long getTransactionId(long timestamp) {
        return timestampTidMapping.get(timestamp);
    }

    @Override
    public LocalTransactionContext newTransaction(T element, long tid) {
        LocalTransactionContext transactionContext = super.newTransaction(element, tid);
        long timestamp = transactionContext.timestamp;
        timestampTidMapping.put(timestamp, tid);
        tidTimestampMapping.computeIfAbsent(tid, k -> new HashSet<>()).add(timestamp);
        return transactionContext;
    }

    @Override
    public void deleteTransaction(long tid) {
        super.deleteTransaction(tid);
        Set<Long> timestamps = tidTimestampMapping.remove(tid);
        timestamps.forEach(ts -> timestampTidMapping.remove(ts));
    }
}

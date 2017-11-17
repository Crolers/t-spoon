package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * Created by affo on 14/07/17.
 * <p>
 * Synchronization is offered by the OpenOperator class.
 */
public class OptimisticOpenOperator<T> extends OpenOperator<T> {
    public final OutputTag<Integer> watermarkTag = new OutputTag<Integer>("watermark") {
    };

    private int lastCommittedWatermark = 0;
    // set of tids depends on tid (tid -> [tids, ...])
    private Map<Integer, Set<Integer>> dependencies = new HashMap<>();
    // timestamp -> current watermark
    private Map<Integer, Integer> playedWithWatermark = new HashMap<>();
    private Set<Integer> laterReplay = new HashSet<>();

    public OptimisticOpenOperator(
            TransactionsIndex<T> transactionsIndex,
            CoordinatorTransactionCloser coordinatorTransactionCloser) {
        super(transactionsIndex, coordinatorTransactionCloser);
    }

    // replay
    private void collect(int tid) {
        T element = transactionsIndex.getTransaction(tid).element;
        TransactionsIndex<T>.LocalTransactionContext tContext = transactionsIndex.newTransaction(element, tid);
        Metadata metadata = new Metadata(tid);
        metadata.timestamp = tContext.timestamp;
        metadata.coordinator = getCoordinatorAddress();
        metadata.watermark = transactionsIndex.getCurrentWatermark();

        onOpenTransaction(element, metadata);
        collector.safeCollect(Enriched.of(metadata, element));
    }

    // called by OpenOperator#processElement
    // and by collect
    @Override
    protected void onOpenTransaction(T recordValue, Metadata metadata) {
        playedWithWatermark.put(metadata.tid, metadata.watermark);
    }

    private void replayElement(Integer tid) {
        // do not replay with the same watermark...
        int playedWithWatermark = this.playedWithWatermark.remove(tid);
        if (playedWithWatermark < transactionsIndex.getCurrentWatermark()) {
            collect(tid);
        } else {
            laterReplay.add(tid);
        }
    }

    private void addDependency(int dependsOn, int tid) {
        dependencies.computeIfAbsent(tid, k -> new HashSet<>()).add(dependsOn);
    }

    // thread-safe
    @Override
    protected void closeTransaction(TransactionsIndex.LocalTransactionContext transactionContext) {
        int tid = transactionContext.tid;
        int timestamp = transactionContext.timestamp;
        Vote vote = transactionContext.vote;
        int replayCause = transactionContext.replayCause;

        Integer dependency = transactionsIndex.getTransactionId(replayCause);

        int oldWM = transactionsIndex.getCurrentWatermark();
        int newWM = transactionsIndex.updateWatermark(timestamp, vote);
        boolean wmUpdate = newWM > oldWM;

        if (wmUpdate) {
            laterReplay.forEach(this::collect);
            laterReplay.clear();
        }

        switch (vote) {
            case COMMIT:
                if (timestamp > lastCommittedWatermark) {
                    lastCommittedWatermark = timestamp;
                }

                if (newWM >= lastCommittedWatermark) {
                    collector.safeCollect(watermarkTag, lastCommittedWatermark);
                }
            case ABORT:
                onTermination(tid);
                break;
            case REPLAY:
                if (dependency != null && tid > dependency && transactionsIndex.getTransaction(dependency) != null) {
                    // the timestamp has a mapping in tids
                    // this transaction depends on a previous one
                    // the transaction on which the current one depends has not commited/aborted
                    addDependency(tid, dependency);
                } else {
                    replayElement(tid);
                }
        }
    }

    private void onTermination(int tid) {
        // cleanup
        transactionsIndex.deleteTransaction(tid);

        Set<Integer> deps = dependencies.remove(tid);
        if (deps != null) {
            // draw a chain of ordered dependencies
            ArrayList<Integer> depList = new ArrayList<>(deps);
            Collections.sort(depList);
            for (int i = 0; i < depList.size() - 1; i++) {
                addDependency(depList.get(i + 1), depList.get(i));
            }

            // replay only the first one
            replayElement(depList.get(0));
        }
    }
}

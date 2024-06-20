package crolers.tgstream.tgraph;

import crolers.tgstream.common.OrderedElements;
import crolers.tgstream.common.OrderedTimestamps;
import crolers.tgstream.common.SimpleOrderedElements;

import java.util.*;

public class TotalOrderEnforcer {
    private long lastRemoved = 0;
    private BatchCompletionChecker completionChecker = new BatchCompletionChecker();
    private OrderedTimestamps timestamps = new OrderedTimestamps();
    private Map<Long, SimpleOrderedElements<BatchID>> bids = new HashMap<>();

    public void addElement(long timestamp, BatchID bid) {
        timestamps.addInOrderWithoutRepetition(timestamp);
        bids.computeIfAbsent(timestamp, ts -> new SimpleOrderedElements<>()).addInOrder(bid);
        completionChecker.checkCompleteness(timestamp, bid);
    }

    public LinkedHashMap<Long, List<BatchID>> next() {
        LinkedHashMap<Long, List<BatchID>> result = new LinkedHashMap<>();

        final long[] newLastRemoved = {lastRemoved};
        timestamps.peekContiguous(lastRemoved,
                timestamp -> {
                    if (!completionChecker.getCompleteness(timestamp)) {
                        return Collections.singleton(OrderedElements.IterationAction.STOP_ITERATION);
                    }

                    completionChecker.freeIndex(timestamp);
                    SimpleOrderedElements<BatchID> completeBid = bids.remove(timestamp);
                    result.put(timestamp, completeBid.toList());
                    newLastRemoved[0] = timestamp;

                    return Collections.singleton(OrderedElements.IterationAction.REMOVE_FROM_ELEMENTS);
                });

        lastRemoved = newLastRemoved[0];
        return result;
    }

    public int getNumberOfIncompleteElements() {
        return timestamps.size();
    }

    public int getTotalNumberOfIncompleteElements() {
        return bids.values().stream()
                .mapToInt(SimpleOrderedElements::size).sum();
    }
}

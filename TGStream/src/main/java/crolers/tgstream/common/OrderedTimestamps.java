package crolers.tgstream.common;

import java.util.ListIterator;

public class OrderedTimestamps extends OrderedElements<Long> {
    public OrderedTimestamps() {
        super((TimestampExtractor<Long>) aLong -> aLong);
    }

    public void addInOrderWithoutRepetition(Long ts) {
        ListIterator<Long> it = iterator();

        boolean added = false;

        while (it.hasNext() && !added) {
            Long nextTimestamp = it.next();
            if (nextTimestamp >= ts) {
                if (nextTimestamp > ts) {
                    it.previous();
                    it.add(ts);
                }
                added = true;
            }
        }

        // add in tail
        if (!added) {
            it.add(ts);
        }
    }
}

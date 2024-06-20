package crolers.tgstream.tgraph.query;

import java.util.concurrent.TimeUnit;

/**
 * Created by crolers
 */
// 附加了每秒查询频率
public class FrequencyQuerySupplier implements QuerySupplier {
    private QuerySupplier wrapped;
    private long waitingIntervalMicro;

    public FrequencyQuerySupplier(QuerySupplier wrapped, double queriesPerSecond) {
        this.wrapped = wrapped;
        this.waitingIntervalMicro = queriesPerSecond == Double.MAX_VALUE ?
                0 :
                (long) ((1.0 / queriesPerSecond) * 1000000);
    }

    @Override
    public Query getQuery(QueryID queryID) {
        if (waitingIntervalMicro > 0) {
            try {
                TimeUnit.MICROSECONDS.sleep(waitingIntervalMicro);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while sleeping");
            }
        }

        return wrapped.getQuery(queryID);
    }
}

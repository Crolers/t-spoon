package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.IsolationLevel;
import crolers.tgstream.tgraph.Metadata;
import crolers.tgstream.tgraph.OTStream;
import crolers.tgstream.tgraph.TransactionEnvironment;
import crolers.tgstream.tgraph.query.MultiStateQuery;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by crolers
 */
public class OptimisticTwoPCFactory implements TwoPCFactory {

    @Override
    public <T> OpenStream<T> open(DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphId) {
        return OTStream.fromStream(ds, queryStream, tGraphId);
    }

    @Override
    public DataStream<Metadata> onClosingSink(
            DataStream<Metadata> votesMerged, TransactionEnvironment transactionEnvironment) {
        if (transactionEnvironment.getIsolationLevel() == IsolationLevel.PL4) {
            return votesMerged.flatMap(new StrictnessEnforcer())
                    .name("StrictnessEnforcer").setParallelism(1);
        }

        return votesMerged;
    }
}

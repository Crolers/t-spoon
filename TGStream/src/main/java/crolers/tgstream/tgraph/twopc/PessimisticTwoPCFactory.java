package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.Metadata;
import crolers.tgstream.tgraph.PTStream;
import crolers.tgstream.tgraph.TransactionEnvironment;
import crolers.tgstream.tgraph.query.MultiStateQuery;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PessimisticTwoPCFactory implements TwoPCFactory {
    @Override
    public <T> OpenStream<T> open(DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphID) {
        return PTStream.fromStream(ds, queryStream, tGraphID);
    }

    @Override
    public DataStream<Metadata> onClosingSink(
            DataStream<Metadata> votesMerged, TransactionEnvironment transactionEnvironment) {
        // does nothing
        return votesMerged;
    }
}

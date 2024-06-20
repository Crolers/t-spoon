package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.Metadata;
import crolers.tgstream.tgraph.TransactionEnvironment;
import crolers.tgstream.tgraph.query.MultiStateQuery;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public interface TwoPCFactory extends Serializable {
    <T> OpenStream<T> open(DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphId);

    DataStream<Metadata> onClosingSink(DataStream<Metadata> votesMerged, TransactionEnvironment transactionEnvironment);
}

package crolers.tgstream.tgraph.backed;

import org.apache.flink.api.java.tuple.Tuple3;

public class Movement extends Tuple3<TransferID, String, Double> {
    public Movement() {
    }

    public Movement(TransferID id, String from, Double amount) {
        super(id, from, amount);
    }


}

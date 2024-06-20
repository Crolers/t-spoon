package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;
import crolers.tgstream.tgraph.db.ObjectHandler;

/**
 * Created by crolers
 */
public class PL2Strategy extends PL1Strategy {

    @Override
    public <V> ObjectHandler<V> readVersion(long tid, long timestamp, long watermark, Object<V> versions) {
        return versions.readCommittedBefore(watermark);
    }
}

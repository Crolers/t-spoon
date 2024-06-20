package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;

/**
 * Created by crolers
 */
public class PL1Strategy extends PL0Strategy {
    @Override
    public boolean canWrite(long tid, long timestamp, long watermark, Object<?> object) {
        long lastVersion = object.getLastAvailableVersion().version;
        return timestamp > lastVersion;
    }
}

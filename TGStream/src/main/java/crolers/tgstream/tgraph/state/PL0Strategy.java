package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;
import crolers.tgstream.tgraph.db.ObjectHandler;
import crolers.tgstream.tgraph.db.ObjectVersion;

/**
 * Created by crolers
 */
public class PL0Strategy implements VersioningStrategy {
    @Override
    public <V> ObjectHandler<V> readVersion(long tid, long timestamp, long watermark, Object<V> versions) {
        return versions.readLastVersionBefore(timestamp);
    }

    @Override
    public boolean canWrite(long tid, long timestamp, long watermark, Object<?> object) {
        return true;
    }

    @Override
    public <V> ObjectVersion<V> installVersion(long tid, long timestamp, Object<V> object, V version) {
        return object.addVersion(tid, timestamp, version);
    }
}

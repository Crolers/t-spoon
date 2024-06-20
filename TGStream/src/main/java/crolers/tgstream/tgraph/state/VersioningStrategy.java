package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;
import crolers.tgstream.tgraph.db.ObjectHandler;
import crolers.tgstream.tgraph.db.ObjectVersion;

import java.io.Serializable;

/**
 * Created by crolers
 * <p>
 * The only versions a transaction can access are previous to its timestamp:
 * the timestamp, indeed, represents a total order on object versions.
 * <p>
 * At PL0 you can read the previous version (before yours) and write when you want.
 * At PL1 you can read the previous version but you can write only if your timestamp
 * is greater than the last version (avoid write cycles).
 * At PL2 you can read the last COMMITTED version and write as in PL1.
 * At PL3 you can read as in PL2, but you can write only if the last version is known to be finished.
 */
public interface VersioningStrategy extends Serializable {
    <V> ObjectHandler<V> readVersion(long tid, long timestamp, long watermark, Object<V> versions);

    boolean canWrite(long tid, long timestamp, long watermark, Object<?> object);

    <V> ObjectVersion<V> installVersion(long tid, long timestamp, Object<V> object, V version);
}

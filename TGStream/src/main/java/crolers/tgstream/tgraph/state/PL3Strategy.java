package crolers.tgstream.tgraph.state;

import crolers.tgstream.tgraph.db.Object;

public class PL3Strategy extends PL2Strategy {
    @Override
    public boolean canWrite(long tid, long timestamp, long watermark, Object<?> object) {
        long lastVersion = object.getLastAvailableVersion().version;
        return watermark >= lastVersion;
    }
}

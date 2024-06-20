package crolers.tgstream.tgraph.durability;

import java.io.IOException;

/**
 * Created by crolers
 */
public class NoSnap implements SnapshotService {
    @Override
    public void open() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void startSnapshot(long newWM) throws IOException {

    }

    @Override
    public long getSnapshotInProgressWatermark() throws IOException {
        return 0;
    }
}

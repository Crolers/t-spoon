package crolers.tgstream.tgraph.durability;

import java.io.IOException;

/**
 * Created by crolers
 */
public interface SnapshotService {
    void open() throws IOException;

    void close() throws IOException;

    void startSnapshot(long newWM) throws IOException;

    long getSnapshotInProgressWatermark() throws IOException;
}

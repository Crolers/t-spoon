package crolers.tgstream.evaluation;

import crolers.tgstream.runtime.NetUtils;
import crolers.tgstream.tgraph.durability.SnapshotClient;
import crolers.tgstream.tgraph.durability.WALClient;
import crolers.tgstream.tgraph.durability.SnapshotService;
import crolers.tgstream.tgraph.durability.WALEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Run this to get the entries replayed and measure replay time
 */
public class DisplayWAL {
    public static void main(String[] args) throws Exception {
        String[] ips = {"localhost"};
        WALClient wal = WALClient.get(ips);
        SnapshotService snapshotService = new SnapshotClient("localhost", NetUtils.GLOBAL_WAL_SERVER_PORT);
        snapshotService.open();

        List<WALEntry> entries = new ArrayList<>();

        long start = System.nanoTime();
        Iterator<WALEntry> replayed = wal.replay(0, 1);
        while (replayed.hasNext()) {
            WALEntry next = replayed.next();
            entries.add(next);
        }
        double delta = (System.nanoTime() - start) / Math.pow(10, 6);

        for (WALEntry entry : entries) {
            System.out.println(entry);
            Map<String, Object> updates = entry.updates.getUpdatesFor("balances", 0);
            System.out.println(updates);
        }

        long wm = snapshotService.getSnapshotInProgressWatermark();

        System.out.println();
        System.out.println(">>> WM:\t\t" + wm);
        System.out.println(">>> n:\t\t" + entries.size());
        System.out.println(">>> ET(ms):\t\t" + delta);
    }
}

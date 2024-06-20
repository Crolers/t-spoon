package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.common.Address;
import crolers.tgstream.runtime.StringClient;
import crolers.tgstream.runtime.StringClientsCache;
import crolers.tgstream.runtime.BatchingStringClient;
import crolers.tgstream.runtime.ClientsCache;
import crolers.tgstream.tgraph.Metadata;
import crolers.tgstream.tgraph.durability.FileWAL;
import crolers.tgstream.tgraph.durability.WALEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by crolers
 */
public abstract class AbstractCloseOperatorTransactionCloser {
    protected transient ClientsCache<StringClient> clients;
    private transient FileWAL wal;
    private final int closeBatchSize;
    private transient ExecutorService pool;
    private transient Exception deferredExecutorException = null;

    public AbstractCloseOperatorTransactionCloser(int closeBatchSize) {
        this.closeBatchSize = closeBatchSize;
    }

    /**
     *
     * @param wal could be null in case durability is not enabled
     * @throws Exception
     */
    public void open(FileWAL wal) throws Exception {
        if (closeBatchSize <= 0) {
            clients = new StringClientsCache();
        } else {
            clients = new ClientsCache<>(
                    address -> new BatchingStringClient(address.ip, address.port, closeBatchSize));
            this.pool = Executors.newFixedThreadPool(1); // the protocol is applied in a deferred way
        }
        this.wal = wal;
    }

    public void close() throws Exception {
        if (deferredExecutorException != null) {
            throw deferredExecutorException;
        }

        clients.clear();

        if (pool != null) {
            pool.shutdown();
        }
    }

    public void onMetadata(Metadata metadata) throws Exception {
        if (deferredExecutorException != null) {
            throw deferredExecutorException;
        }

        // write before applying protocol
        if (wal != null) {
            wal.addEntry(new WALEntry(metadata.vote, metadata.tid, metadata.timestamp, metadata.updates));
        }

        if (pool == null) {
            applyProtocolOnMetadata(metadata);
        } else {
            pool.submit(() -> {
                try {
                    applyProtocolOnMetadata(metadata);
                } catch (Exception e) {
                    deferredExecutorException = e;
                }
            });
        }
    }

    /**
     * Invoked every time a new transaction result has been gathered
     *
     * @param metadata
     */
    abstract void applyProtocolOnMetadata(Metadata metadata) throws Exception;

    // ------------------------------ helper methods

    protected void send(Address address, String message) throws IOException {
        send(Collections.singleton(address), message);
    }

    protected void send(Iterable<Address> addresses, String message) throws IOException {
        for (Address address : addresses) {
            clients.getOrCreateClient(address).send(message);
        }
    }
}

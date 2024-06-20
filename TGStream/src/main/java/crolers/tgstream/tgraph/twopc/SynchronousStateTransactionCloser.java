package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.common.Address;
import crolers.tgstream.runtime.StringClient;
import crolers.tgstream.runtime.StringClientsCache;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class SynchronousStateTransactionCloser extends AbstractStateOperatorTransactionCloser {
    private transient StringClientsCache clientsCache;
    private transient ExecutorService pool;

    protected SynchronousStateTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    public void open() throws Exception {
        super.open();
        clientsCache = new StringClientsCache();
        // ... with only 1 thread we ensure that we preserve order
        pool = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        super.close();
        clientsCache.clear();
        pool.shutdown();
    }

    // called once per TM
    @Override
    protected void onClose(Address coordinatorAddress, String request,
                           Consumer<Void> onSinkACK, Consumer<Void> onCoordinatorACK,
                           Consumer<Throwable> error) {
        onSinkACK.accept(null);

        StringClient coordinator;
        try {
            coordinator = clientsCache.getOrCreateClient(coordinatorAddress);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot connect to coordinator: " + coordinatorAddress);
        }

        coordinator.send(request);
        pool.submit(() -> {
            try {
                // wait for the ACK
                coordinator.receive();
                onCoordinatorACK.accept(null);
            } catch (IOException e) {
                error.accept(e);
            }
        });
    }
}

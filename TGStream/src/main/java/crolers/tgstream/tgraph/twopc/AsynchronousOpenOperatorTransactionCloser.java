package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.runtime.AbstractServer;
import crolers.tgstream.runtime.ProcessRequestServer;

/**
 * Created by crolers
 */
public class AsynchronousOpenOperatorTransactionCloser extends AbstractOpenOperatorTransactionCloser {
    protected AsynchronousOpenOperatorTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    protected AbstractServer getServer() {
        return new OpenServer();
    }

    private class OpenServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            notifyListeners(notification, (listener) -> listener.onCloseTransaction(notification));
        }
    }
}

package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.common.Address;

import java.util.function.Consumer;

/**
 * Created by crolers
 */
public class AsynchronousStateTransactionCloser extends AbstractStateOperatorTransactionCloser {
    protected AsynchronousStateTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    protected void onClose(Address coordinatorAddress, String request,
                           Consumer<Void> onSinkACK, Consumer<Void> onCoordinatorACK,
                           Consumer<Throwable> error) {
        onSinkACK.accept(null);
        onCoordinatorACK.accept(null);
    }
}

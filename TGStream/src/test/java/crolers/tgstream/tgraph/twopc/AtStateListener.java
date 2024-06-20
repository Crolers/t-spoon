package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.common.Address;

/**
 * Created by crolers
 */
public class AtStateListener extends AbstractListener<StateOperatorTransactionCloseListener>
        implements StateOperatorTransactionCloseListener {
    public final static String prefix = ">> AtState:\t";
    private Address coordinatorAddress;

    public AtStateListener(
            Address coordinatorAddress,
            AbstractTwoPCParticipant<StateOperatorTransactionCloseListener> closer,
            AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        super(closer, subscriptionMode);
        this.coordinatorAddress = coordinatorAddress;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public StateOperatorTransactionCloseListener getListener() {
        return this;
    }

    @Override
    public void onTransactionClosedSuccess(CloseTransactionNotification notification) {
        queue.addMessage(notification);
    }

    @Override
    public void onTransactionClosedError(CloseTransactionNotification notification, Throwable error) {
        System.out.println(prefix + notification + " - " + error.getMessage());
    }

    @Override
    public Address getCoordinatorAddressForTransaction(long timestamp) {
        return coordinatorAddress;
    }

    @Override
    public boolean isInterestedIn(long timestamp) {
        return subscriber.isInterestedIn(timestamp);
    }

    @Override
    public int getTGraphID() {
        return 0;
    }
}

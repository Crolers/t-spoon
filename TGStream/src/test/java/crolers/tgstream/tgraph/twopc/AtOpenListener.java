package crolers.tgstream.tgraph.twopc;

public class AtOpenListener extends AbstractListener<OpenOperatorTransactionCloseListener>
        implements OpenOperatorTransactionCloseListener {
    public final static String prefix = "> AtOpen:\t";

    public AtOpenListener(
            AbstractTwoPCParticipant<OpenOperatorTransactionCloseListener> closer,
            AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        super(closer, subscriptionMode);
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public OpenOperatorTransactionCloseListener getListener() {
        return this;
    }

    @Override
    public void onCloseTransaction(CloseTransactionNotification notification) {
        queue.addMessage(notification);
    }

    @Override
    public int getTGraphID() {
        return 0;
    }
}

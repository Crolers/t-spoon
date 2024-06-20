package crolers.tgstream.tgraph.twopc;

/**
 * Created by crolers
 */
public interface OpenOperatorTransactionCloseListener extends TwoPCParticipant.Listener {
    void onCloseTransaction(CloseTransactionNotification notification);
}

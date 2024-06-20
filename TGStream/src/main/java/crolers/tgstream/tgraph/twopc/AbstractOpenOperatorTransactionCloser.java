package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.runtime.NetUtils;
import crolers.tgstream.runtime.AbstractServer;

import java.util.function.Supplier;

public abstract class AbstractOpenOperatorTransactionCloser extends
        AbstractTwoPCParticipant<OpenOperatorTransactionCloseListener> {

    protected AbstractOpenOperatorTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    public synchronized void open() throws Exception {
        super.open();
    }

    @Override
    public NetUtils.ServerType getServerType() {
        return NetUtils.ServerType.OPEN;
    }

    @Override
    public Supplier<AbstractServer> getServerSupplier() {
        return this::getServer;
    }

    protected abstract AbstractServer getServer();
}

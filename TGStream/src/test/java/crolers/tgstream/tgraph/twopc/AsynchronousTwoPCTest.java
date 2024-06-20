package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.TransactionEnvironment;

public class AsynchronousTwoPCTest extends SimpleTwoPCTest {

    @Override
    protected void configureTransactionalEnvironment(TransactionEnvironment tEnv) {
        tEnv.setSynchronous(false);
    }
}

package crolers.tgstream.tgraph.twopc;

import crolers.tgstream.tgraph.TransactionEnvironment;

public class SynchronousTwoPCTest extends SimpleTwoPCTest {

    @Override
    protected void configureTransactionalEnvironment(TransactionEnvironment tEnv) {
        tEnv.setSynchronous(true);
    }
}

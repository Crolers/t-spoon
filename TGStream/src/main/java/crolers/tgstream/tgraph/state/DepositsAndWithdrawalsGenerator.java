package crolers.tgstream.tgraph.state;

import crolers.tgstream.common.RPC;

import java.util.Random;

public class DepositsAndWithdrawalsGenerator implements RandomSPUSupplier {
    public final int keyspaceSize;
    public final String keyPrefix, namespace;
    private final double startAmount;

    public DepositsAndWithdrawalsGenerator(
            String namespace, String keyPrefix, int keyspaceSize, double startAmount) {
        this.keyspaceSize = keyspaceSize;
        this.keyPrefix = keyPrefix;
        this.namespace = namespace;
        this.startAmount = startAmount;
    }

    @Override
    public SinglePartitionUpdate next(SinglePartitionUpdateID spuID, Random random) {
        String methodName = random.nextBoolean() ? "deposit" : "withdrawal";
        double amount = Math.ceil(random.nextDouble() * startAmount);
        String key = keyPrefix + random.nextInt(keyspaceSize);

        RPC rpc = new RPC(methodName, amount);
        return new SinglePartitionUpdate(spuID, namespace, key, rpc);
    }
}

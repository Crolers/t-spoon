package crolers.tgstream.tgraph.backed;

import crolers.tgstream.common.RandomProvider;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Random;

public class Transfer extends Tuple4<TransferID, String, String, Double> {
    private static Random random = RandomProvider.get();
    public static final String KEY_PREFIX = "a";

    public Transfer() {
    }

    // 一次转账的属性字段包括 TransferID, from 账户、to 账户、amount
    public Transfer(TransferID id, String from, String to, Double amount) {
        super(id, from, to, amount);
    }

    public Movement getDeposit() {
        return new Movement(this.f0, this.f1, -this.f3);
    }

    public Movement getWithdrawal() {
        return new Movement(this.f0, this.f2, this.f3);
    }

    public static Transfer generateTransfer(TransferID id, int noAccounts, double startAmount) {
        return Transfer.generateTransfer(id, noAccounts, startAmount, random);
    }

    public static Transfer generateTransfer(TransferID id, int noAccounts, double startAmount, Random random) {
        String from = KEY_PREFIX + random.nextInt(noAccounts);
        String to;
        do {
            to = KEY_PREFIX + random.nextInt(noAccounts);
        } while (from.equals(to));
        Double amount = Math.ceil(random.nextDouble() * startAmount);

        return new Transfer(id, from, to, amount);
    }
}

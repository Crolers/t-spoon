package crolers.tgstream.evaluation;

import crolers.tgstream.common.FinishOnCountSink;
import crolers.tgstream.common.SinglePartitionCommand;
import crolers.tgstream.runtime.NetUtils;
import crolers.tgstream.tgraph.TStream;
import crolers.tgstream.tgraph.backed.TransferSource;
import crolers.tgstream.tgraph.backed.TunableSPUSource;
import crolers.tgstream.tgraph.db.ObjectHandler;
import crolers.tgstream.tgraph.query.FrequencyQuerySupplier;
import crolers.tgstream.tgraph.state.*;
import crolers.tgstream.common.FlatMapFunction;
import crolers.tgstream.tgraph.TransactionEnvironment;
import crolers.tgstream.tgraph.TransactionResult;
import crolers.tgstream.tgraph.backed.Movement;
import crolers.tgstream.tgraph.backed.Transfer;
import crolers.tgstream.tgraph.query.PredicateQuery;
import crolers.tgstream.tgraph.query.QueryResultMerger;
import crolers.tgstream.tgraph.query.RandomQuerySupplier;
import crolers.tgstream.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class BankUseCase {
    public static final String SPU_TRACKING_SERVER_NAME = "spu-tracker";
    public static final double startAmount = 100;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);

        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final double queryFrequency = parameters.getDouble("queryRate", 100);
        final long inputWaitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);


        final int averageQuerySize = parameters.getInt("avg", 10);
        final int stdDevQuerySize = parameters.getInt("stddev", 5);

        final boolean consistencyCheck = parameters.getBoolean("check", false);

        //according to IP param init a JobControl server
        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        // TransferSource 生成转账tuple（频率为 100 次\s）
        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, config.keySpaceSize, startAmount);
        transferSource.setMicroSleep(inputWaitPeriodMicro);

        if (consistencyCheck) {
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            queryID -> new PredicateQuery<>(nameSpace, queryID, new PredicateQuery.SelectAll<>()),
                            0.1));
        } else {
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            new RandomQuerySupplier(nameSpace, 0, Transfer.KEY_PREFIX, config.keySpaceSize,
                                    averageQuerySize, stdDevQuerySize), queryFrequency));
            // Uncomment if you want query results
             tEnv.setOnQueryResult(new QueryResultMerger.PrintQueryResult());
        }

        RandomSPUSupplier spuSupplier = new DepositsAndWithdrawalsGenerator(
                nameSpace, Transfer.KEY_PREFIX, config.keySpaceSize, startAmount
        );
        TunableSPUSource tunableSPUSource = new TunableSPUSource(config, SPU_TRACKING_SERVER_NAME, spuSupplier);

        SingleOutputStreamOperator<SinglePartitionUpdate> spuStream = env.addSource(tunableSPUSource)
                .name("TunableSPUSource");

        tEnv.enableSPUpdates(spuStream);

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);
        OpenStream<Transfer> open = tEnv.open(transfers);

        //Transfer 类型作为输入，Movement 类型作为输出。在这个 Lambda 表达式中，我们将一个 Transfer 对象转换为两个 Movement 对象，
        //分别来自 Deposit 和 Withdrawal 属性，并将这两个对象作为列表的形式返回。flatMap 再将这个列表扁平化为一个 Movement 流
        TStream<Movement> halves = open.opened.flatMap(
                //Lambda 表达式的类型强转
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        //状态查询
        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new Balances(), config.partitioning);


        DataStream<TransactionResult> output = tEnv.close(balances.leftUnchanged);

        balances.spuResults
                .map(tr -> ((SinglePartitionUpdate) tr.f2).id).returns(SinglePartitionUpdateID.class)
                .addSink(new Tracker<>(SPU_TRACKING_SERVER_NAME))
                .name("EndTracker")
                .setParallelism(1);

        if (consistencyCheck) {
            balances.queryResults.addSink(new ConsistencyCheck.CheckOnQueryResult(startAmount));
            FinishOnCountSink<TransactionResult> finishOnCountSink = new FinishOnCountSink<>(config.batchSize);
            finishOnCountSink.notSevere();
            output.addSink(finishOnCountSink)
                    .name("FinishOnCount")
                    .setParallelism(1);
        }

        String label = config.strategy + " - " + config.isolationLevel + ": ";

        if (consistencyCheck) {
            label += "Bank example consistency check";
        } else {
            label += "Bank example SPU evaluation";
        }

        env.execute(label);
    }

    private static class Balances implements StateFunction<Movement, Double> {
        @Override
        public Double defaultValue() {
            return startAmount;
        }

        @Override
        public Double copyValue(Double balance) {
            return balance;
        }

        @Override
        public boolean invariant(Double balance) {
            return balance >= 0;
        }

        @Override
        public void apply(Movement element, ObjectHandler<Double> handler) {
            // this is the transaction:
            // r(x) w(x)
            handler.write(handler.read() + element.f2);
        }

        @SinglePartitionCommand
        public double deposit(double amount, double currentBalance) {
            return currentBalance + amount;
        }

        @SinglePartitionCommand
        public double withdrawal(double amount, double currentBalance) {
            return currentBalance - amount;
        }
    }
}

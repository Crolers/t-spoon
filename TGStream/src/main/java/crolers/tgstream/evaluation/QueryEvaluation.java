package crolers.tgstream.evaluation;

import crolers.tgstream.runtime.NetUtils;
import crolers.tgstream.tgraph.TStream;
import crolers.tgstream.tgraph.backed.TransferSource;
import crolers.tgstream.tgraph.db.ObjectHandler;
import crolers.tgstream.tgraph.query.MultiStateQuery;
import crolers.tgstream.tgraph.query.QueryID;
import crolers.tgstream.tgraph.state.StateFunction;
import crolers.tgstream.common.FlatMapFunction;
import crolers.tgstream.tgraph.TransactionEnvironment;
import crolers.tgstream.tgraph.backed.Movement;
import crolers.tgstream.tgraph.backed.Transfer;
import crolers.tgstream.tgraph.backed.TunableQuerySource;
import crolers.tgstream.tgraph.query.Query;
import crolers.tgstream.tgraph.state.StateStream;
import crolers.tgstream.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Created by crolers
 */
public class QueryEvaluation {
    public static final String QUERY_TRACKING_SERVER_NAME = "query-tracker";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int averageQuerySize = parameters.getInt("avg", 1);
        final int stdDevQuerySize = parameters.getInt("stddev", 0);

        Preconditions.checkArgument(averageQuerySize < config.keySpaceSize);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, config.keySpaceSize, EvalConfig.startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        TunableQuerySource tunableQuerySource = new TunableQuerySource(
                config, QUERY_TRACKING_SERVER_NAME, nameSpace,
                config.keySpaceSize, averageQuerySize, stdDevQuerySize);

        SingleOutputStreamOperator<Query> queries = env.addSource(tunableQuerySource);
        queries = config.addToSourcesSharingGroup(queries, "TunableQuerySource");

        SingleOutputStreamOperator<MultiStateQuery> msQueries = queries.map(q -> {
            MultiStateQuery multiStateQuery = new MultiStateQuery();
            multiStateQuery.addQuery(q);
            return multiStateQuery;
        });

        msQueries = config.addToSourcesSharingGroup(msQueries, "ToMultiStateQuery");

        tEnv.enableCustomQuerying(msQueries);

        DataStream<Transfer> transfers = env.addSource(transferSource)
                .slotSharingGroup(config.sourcesSharingGroup).setParallelism(1);

        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new StateFunction<Movement, Double>() {
                    @Override
                    public Double defaultValue() {
                        return EvalConfig.startAmount;
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
                }, config.partitioning);

        balances.queryResults
                .map(qr -> qr.queryID).returns(QueryID.class)
                .addSink(new Tracker<>(QUERY_TRACKING_SERVER_NAME))
                .name("EndTracker")
                .setParallelism(1);


        tEnv.close(balances.leftUnchanged);

        env.execute("Query evaluation at " + config.strategy + " - " + config.isolationLevel);
    }
}

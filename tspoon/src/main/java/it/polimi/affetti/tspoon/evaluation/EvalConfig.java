package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * Created by affo on 02/05/18.
 */
public class EvalConfig {
    public static final double startAmount = 100d;
    public static final String sourceSharingGroup = "sources";

    public ParameterTool params;
    public String label;
    public int sourcePar;
    public IsolationLevel isolationLevel;
    public int parallelism;
    public int partitioning;
    public int noTGraphs;
    public int noStates;
    public int keySpaceSize;
    public boolean seriesOrParallel;
    public boolean useDependencyTracking;
    public boolean synchronous;
    public boolean durable;
    public int openServerPoolSize, stateServerPoolSize, queryServerPoolSize;
    public boolean printPlan;
    public boolean baselineMode;
    public int batchSize;
    public int resolution;
    public int maxNumberOfBatches;
    public Strategy strategy;
    public int startInputRate;
    public String propertiesFile;
    public boolean isLocal;
    public String[] taskManagerIPs;
    public int closeBatchSize;

    private StreamExecutionEnvironment flinkEnv;

    public static EvalConfig fromParams(ParameterTool parameters) throws IOException {
        EvalConfig config = new EvalConfig();

        config.label = parameters.get("label");
        config.isLocal = config.label == null || config.label.startsWith("local");
        config.propertiesFile = parameters.get("propsFile", null);
        config.sourcePar = parameters.getInt("sourcePar", 1);
        // let the real source parallelism affect the overall parallelism
        config.parallelism = parameters.getInt("par", 4) - config.sourcePar;
        config.partitioning = parameters.getInt("partitioning", 4) - config.sourcePar;
        int isolationLevelNumber = parameters.getInt("isolationLevel", 3);
        config.isolationLevel = IsolationLevel.values()[isolationLevelNumber];

        // At PL4 we need an order defined by the user, the source has thus parallelism 1.
        // The overall parallelism remains the user-define one minus the original sourcePar
        // for proper comparison with other experiments.
        if (config.isolationLevel == IsolationLevel.PL4) {
            config.sourcePar = 1;
        }

        config.noTGraphs = parameters.getInt("noTG", 1);
        config.noStates = parameters.getInt("noStates", 1);
        config.keySpaceSize = parameters.getInt("ks", 100000);
        config.seriesOrParallel = parameters.getBoolean("series", true);
        boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        config.strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        config.useDependencyTracking = parameters.getBoolean("dependencyTracking", true);
        config.synchronous = parameters.getBoolean("synchronous", false);
        config.durable = parameters.getBoolean("durable", false);
        config.openServerPoolSize = parameters.getInt("openPool", 1);
        config.stateServerPoolSize = parameters.getInt("statePool", 1);
        config.queryServerPoolSize = parameters.getInt("queryPool", 1);
        config.batchSize = parameters.getInt("batchSize", 100000);
        config.resolution = parameters.getInt("resolution", 100);
        config.maxNumberOfBatches = parameters.getInt("numberOfBatches", -1);
        config.startInputRate = parameters.getInt("startInputRate", -1);
        config.startInputRate = getStartRateFromProps(config);
        String ipsCsv = parameters.get("taskmanagers", "localhost");
        config.taskManagerIPs = ipsCsv.split(",");
        config.closeBatchSize = parameters.getInt("closeBatchSize", 0);


        // debugging stuff
        config.printPlan = parameters.getBoolean("printPlan", false);
        config.baselineMode = parameters.getBoolean("baseline", false);

        // merge the value obtained for startInputRate for later checking
        parameters.toMap().put("startInputRate", String.valueOf(config.startInputRate));
        config.params = parameters;

        config.createFlinkEnv();

        return config;
    }

    public static int getStartRateFromProps(EvalConfig config) throws IOException {
        // get startInputRate from props file -> this enables us to run the experiments faster
        int startInputRate = config.startInputRate;
        if (startInputRate < 0) {
            if (config.propertiesFile != null) {
                ParameterTool fromProps = ParameterTool.fromPropertiesFile(config.propertiesFile);

                // try to use the composite key first
                String strStrategy = config.strategy == Strategy.OPTIMISTIC ? "TB" : "LB"; // timestamp-based or lock-based
                String key = String.format("%s.%s.%s", config.label, strStrategy, config.isolationLevel.toString());
                startInputRate = fromProps.getInt(key, -1);

                // use only the label otherwise
                if (startInputRate < 0) {
                    startInputRate = fromProps.getInt(config.label, -1);
                }
            }

            if (startInputRate < 0) {
                startInputRate = 1000; // set to default
            }
        }

        return startInputRate;
    }

    private void createFlinkEnv() {
        StreamExecutionEnvironment env;
        if (isLocal) { // TODO the only way to make slot sharing group work locally for now...
            Configuration conf = new Configuration();
            conf.setInteger("taskmanager.numberOfTaskSlots", parallelism + sourcePar);
            env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Flink suggests to keep it within 5 and 10 ms:
        // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html#controlling-latency
        // Anyway, we keep it to 0 to be as reactive as possible when new records are produced.
        // Buffering could lead to unexpected behavior (in terms of performance) in the transaction management.
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        env.getConfig().setGlobalJobParameters(params);
        this.flinkEnv = env;
    }

    public StreamExecutionEnvironment getFlinkEnv() {
        return flinkEnv;
    }

    public <T> SingleOutputStreamOperator<T> addToSourcesSharingGroup(
            SingleOutputStreamOperator<T> ds, String operatorName) {
        return ds
                .name(operatorName).setParallelism(sourcePar)
                .slotSharingGroup(sourceSharingGroup);
    }
}

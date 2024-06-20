package crolers.tgstream.evaluation;

import crolers.tgstream.metrics.MetricAccumulator;
import crolers.tgstream.metrics.MetricCurveAccumulator;
import crolers.tgstream.runtime.JobControlClient;
import crolers.tgstream.runtime.ProcessRequestServer;
import crolers.tgstream.runtime.WithServer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * Sends the ping to keep a rate at source.
 */
public abstract class TunableSource<T extends UniquelyRepresentableForTracking>
        extends RichSourceFunction<T> {
    public static final long ABORT_TIMEOUT = 3 * 60 * 1000;
    public static final String TARGETING_CURVE_ACC = "targeting-curve";
    public static final String LATENCY_UNLOADED_ACC = "latency-unloaded";
    public static final String INPUT_RATE_ACC = "sustainable-workload";
    public static final String BACKPRESSURE_THROUGHPUT_ACC = "backpressure-throughput";

    protected transient Logger LOG;
    protected int taskNumber = 0; // for portability in case of parallel source

    private transient Timer timer;
    private boolean busyWait = true;
    private final int numberOfSamplesUnloaded;
    private final long backPressureBatchSize, unloadedBatchSize, targetingBatchSize;
    protected int count;
    private int recordsInQueue;
    private double resolution;
    private volatile boolean batchOpen = true;
    private transient MetricCalculator metricCalculator;

    // accumulators
    private final MetricCurveAccumulator targetingCurve;
    private final MetricAccumulator latencyUnloaded, sustainableWorkload, backpressureThroughput;

    private transient JobControlClient jobControlClient;
    private transient WithServer requestTracker;
    private final String trackingServerNameForDiscovery;

    public TunableSource(EvalConfig config, String trackingServerNameForDiscovery) {
        this.backPressureBatchSize = config.backPressureBatchSize;
        this.unloadedBatchSize = config.unloadedBatchSize;
        this.targetingBatchSize = config.targetingBatchSize;
        this.recordsInQueue = config.recordsInQueue;
        this.numberOfSamplesUnloaded = config.numberOfSamplesUnloaded;
        this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
        this.count = 0;
        this.resolution = config.resolution;

        this.targetingCurve = new MetricCurveAccumulator();
        this.latencyUnloaded = new MetricAccumulator();
        this.backpressureThroughput = new MetricAccumulator();
        this.sustainableWorkload = new MetricAccumulator();
    }

    public void disableBusyWait() {
        this.busyWait = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger("TunableSource");

        timer = new Timer("BatchingTimer");
        metricCalculator = new MetricCalculator();

        getRuntimeContext().addAccumulator(TARGETING_CURVE_ACC, targetingCurve);
        getRuntimeContext().addAccumulator(LATENCY_UNLOADED_ACC, latencyUnloaded);
        getRuntimeContext().addAccumulator(INPUT_RATE_ACC, sustainableWorkload);
        getRuntimeContext().addAccumulator(BACKPRESSURE_THROUGHPUT_ACC, backpressureThroughput);

        requestTracker = new WithServer(new TrackingServer());
        requestTracker.open();

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.registerServer(trackingServerNameForDiscovery, requestTracker.getMyAddress());
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
        requestTracker.close();
    }

    private void startBatch(long lengthMilliseconds) {
        batchOpen = true;
        metricCalculator.start();
        TimerTask batchTask = new TimerTask() {
            @Override
            public void run() {
                batchOpen = false;
            }
        };
        //schedule(TimerTask task, long delay)：在当前时间的基础上，延迟指定时间后执行 TimerTask 任务
        timer.schedule(batchTask, lengthMilliseconds);
    }

    private SourceContext<T> context;

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        this.context = sourceContext;

        runInitPhase();

        double latencyUnloaded = getLatencyUnloaded();
        double latencyThreshold = latencyUnloaded * recordsInQueue;
        LOG.info(">>> Latency unloaded is " + latencyUnloaded);
        LOG.info(">>> Latency threshold is " + latencyThreshold);
        double maxThroughput = runBackpressurePhase();

        if (maxThroughput < resolution) {
            resolution = maxThroughput / 2;
        }

        double sustainableWorkload = runTargetingPhase(maxThroughput, latencyThreshold);
        this.sustainableWorkload.add(sustainableWorkload);

        //得到最大可持续工作负载
        LOG.info(">>> Evaluation completed, sustainable workload is " + sustainableWorkload);
        jobControlClient.publishFinishMessage();
    }

    private void runInitPhase() throws TimeoutException, InterruptedException {
        LOG.info(">>> Sending init batches");
        // a pair of batches, just to be sure that every connection is set
        launchUnloadedBatch();
        launchUnloadedBatch();
    }

    private double getLatencyUnloaded() throws TimeoutException, InterruptedException {
        LOG.info(">>> Latency unloaded phase");
        for (int i = 0; i < numberOfSamplesUnloaded; i++) {
            LOG.info(">>> Sending unloaded batch [" + (i + 1) + "/" + numberOfSamplesUnloaded + "]");
            MetricCalculator.Measurement mUnloaded = launchUnloadedBatch();
            LOG.info(">>> Unloaded data: " + mUnloaded);
            latencyUnloaded.add(mUnloaded.latency);
        }
        return latencyUnloaded.getLocalValue().metric.getMean();
    }

    private double runBackpressurePhase() throws TimeoutException, InterruptedException {
        LOG.info(">>> Backpressure phase, sending at maximum rate");
        MetricCalculator.Measurement mMax = launchMaxBatch();
        backpressureThroughput.add(mMax.throughput);
        LOG.info(">>> Overloaded data: " + mMax);
        return mMax.throughput;
    }

    private double runTargetingPhase(double maxThroughput, double latencyThreshold) throws InterruptedException {
        LOG.info(">>> Starting targeting phase with target latency " + latencyThreshold + "[ms]");
        double currentLatency, currentRate = maxThroughput, sustainableWorkload = maxThroughput;
        boolean stop = false;
        do {
            MetricCalculator.Measurement m = launchBatch(currentRate, targetingBatchSize);

            if (!m.isValid()) {
                // the system is overloaded
                break;
            }

            //延迟小于阈值，不断增加输入速率
            currentLatency = m.latency;
            currentRate += resolution;

            // 延迟未超过阈值，更新当前可持续工作负载
            if (currentLatency < latencyThreshold) {
                sustainableWorkload = m.inputRate;
            } else {
                stop = true;
            }

            LOG.info(">>> Targeting data: " + m);
            targetingCurve.add(Point.of(m.inputRate, currentRate, m.latency, m.throughput));
        } while (!stop);

        return sustainableWorkload;
    }

    //以指定速率输入元素
    private MetricCalculator.Measurement launchBatch(double rate, long size) throws InterruptedException {
        startBatch(size);
        emitRecordsAtRate(rate);
        return metricCalculator.end();
    }

    private MetricCalculator.Measurement launchUnloadedBatch() throws InterruptedException, TimeoutException {
        startBatch(unloadedBatchSize);
        emitRecordsOneByOne();
        return metricCalculator.end();
    }

    private MetricCalculator.Measurement launchMaxBatch() throws InterruptedException, TimeoutException {
        startBatch(backPressureBatchSize);
        emitRecordsAtMaxRate();
        return metricCalculator.endAfterCompletion(ABORT_TIMEOUT);
    }

    private void emitRecordsAtMaxRate() {
        while (batchOpen) {
            T next = getNext(count++);
            metricCalculator.sent(next.getUniqueRepresentation());
            context.collect(next);
        }
    }

    private void emitRecordsAtRate(double currentRate) throws InterruptedException {
        long waitPeriodMicro = (long) (Math.pow(10, 6) / currentRate);
        long start = System.nanoTime();
        long localCount = 0;

        while (batchOpen) {
            T next = getNext(count++);

            metricCalculator.sent(next.getUniqueRepresentation());
            context.collect(next);
            localCount++;

            long timeForNext = waitPeriodMicro * localCount;
            long deltaFromBeginning = (long) ((System.nanoTime() - start) * Math.pow(10, -3));
            long stillToSleep = timeForNext - deltaFromBeginning;

            sleep(stillToSleep);
        }
    }

    private void emitRecordsOneByOne() throws InterruptedException, TimeoutException {
        while (batchOpen) {
            T next = getNext(count++);
            metricCalculator.sent(next.getUniqueRepresentation());
            context.collect(next);
            metricCalculator.waitForEveryRecordUntilThisPoint(ABORT_TIMEOUT);
        }
    }

    private void sleep(long stillToSleep) throws InterruptedException {
        if (stillToSleep < 0) {
            return;
        }

        if (busyWait) {
            long start = System.nanoTime();
            while (System.nanoTime() - start < stillToSleep * 1000) {
                // busy loop B)
            }
        } else {
            TimeUnit.MICROSECONDS.sleep(stillToSleep);
        }
    }

    protected abstract T getNext(int count);

    @Override
    public void cancel() {
    }

    private class TrackingServer extends ProcessRequestServer {
        /**
         * The input string must be a UniqueRepresentation
         * @param recordID
         */
        @Override
        protected void parseRequest(String recordID) {
            metricCalculator.received(recordID);
        }
    }

    private static class Point extends MetricCurveAccumulator.Point {
        public final double actualRate, expectedRate;
        public final double latency, throughput;

        public Point(double actualRate, double expectedRate, double latency, double throughput) {
            this.actualRate = actualRate;
            this.expectedRate = expectedRate;
            this.latency = latency;
            this.throughput = throughput;
        }

        public static Point of(double actualRate, double expectedRate, double latency, double throughput) {
            return new Point(actualRate, expectedRate, latency, throughput);
        }

        @Override
        public JSONObject toJSON() {
            Map<String, Object> map = new HashMap<>();
            map.put("actualRate", actualRate);
            map.put("expectedRate", expectedRate);
            map.put("latency", latency);
            map.put("throughput", throughput);
            return new JSONObject(map);
        }
    }
}

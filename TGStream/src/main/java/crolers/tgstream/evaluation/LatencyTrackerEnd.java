package crolers.tgstream.evaluation;

import crolers.tgstream.metrics.TimeDelta;
import crolers.tgstream.runtime.JobControlClient;
import crolers.tgstream.runtime.ProcessRequestServer;
import crolers.tgstream.runtime.WithServer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class LatencyTrackerEnd<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
    private transient WithServer requestTracker;
    private transient JobControlClient jobControlClient;
    private final String trackingServerName;
    private final String accumulatorName;
    private TimeDelta currentLatency;
    private boolean firstStart = false;

    public LatencyTrackerEnd(String trackingServerName, String accumulatorName) {
        this.trackingServerName = trackingServerName;
        this.accumulatorName = accumulatorName;
        this.currentLatency = new TimeDelta();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        requestTracker = new WithServer(new ProcessRequestServer() {
            @Override
            protected void parseRequest(String recordID) {
                //收到请求即开始计算延迟
                start(recordID);
            }
        });
        //启动 server 线程
        requestTracker.open();
        //服务注册，用于后续的 discover
        jobControlClient.registerServer(trackingServerName, requestTracker.getMyAddress());

        getRuntimeContext().addAccumulator(accumulatorName, currentLatency.getNewAccumulator());
    }

    @Override
    public void close() throws Exception {
        super.close();
        requestTracker.close();
        jobControlClient.close();
    }

    @Override
    public synchronized void invoke(T t) throws Exception {
        if (firstStart) {
            currentLatency.end(t.getUniqueRepresentation());
        }
    }

    private synchronized void start(String recordID) {
        //延迟计算：开始计时
        currentLatency.start(recordID);
        firstStart = true;
    }
}
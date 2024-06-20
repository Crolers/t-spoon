package crolers.tgstream.evaluation;

import crolers.tgstream.common.Address;
import crolers.tgstream.runtime.JobControlClient;
import crolers.tgstream.runtime.StringClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by crolers
 */
public class LatencyTrackerStart<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
    private transient StringClient requestTrackerClient;
    private transient JobControlClient jobControlClient;
    private String trackingServerNameForDiscovery;

    private TransientPeriod transientPeriod;

    public LatencyTrackerStart(String trackingServerNameForDiscovery, TransientPeriod transientPeriod) {
        this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
        this.transientPeriod = transientPeriod;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        Address address = jobControlClient.discoverServer(trackingServerNameForDiscovery);
        requestTrackerClient = new StringClient(address.ip, address.port);
        requestTrackerClient.init();

        transientPeriod.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
        requestTrackerClient.close();
    }

    @Override
    public void invoke(T element) throws Exception {
        if (transientPeriod.hasFinished()) {
            requestTrackerClient.send(element.getUniqueRepresentation());
        }
    }
}
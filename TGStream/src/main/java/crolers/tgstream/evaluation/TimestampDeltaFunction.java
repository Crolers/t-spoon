package crolers.tgstream.evaluation;

import crolers.tgstream.metrics.Report;
import crolers.tgstream.metrics.TimeDelta;
import crolers.tgstream.tgraph.backed.TransferID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

/**
 * Created by crolers
 * <p>
 * Outputs the measurements performed for further processing.
 */
public class TimestampDeltaFunction extends
        RichFlatMapFunction<Tuple2<TransferID, Boolean>, Tuple2<String, Double>> {
    public static final String LATENCY_ACC = "latency";

    private transient Logger LOG;
    private TimeDelta timeDelta;

    public TimestampDeltaFunction() {
        timeDelta = new TimeDelta();

        Report.registerAccumulator(LATENCY_ACC);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        getRuntimeContext().addAccumulator(LATENCY_ACC, timeDelta.getNewAccumulator());
    }

    @Override
    public void flatMap(
            Tuple2<TransferID, Boolean> toTrack,
            Collector<Tuple2<String, Double>> collector) throws Exception {
        boolean isBegin = toTrack.f1;
        String id = toTrack.f0.toString();

        if (isBegin) {
            boolean newPointGenerated = timeDelta.start(id);
            if (newPointGenerated) {
                LOG.warn("End before start");
            }
        } else {
            timeDelta.end(id);
        }
    }
}

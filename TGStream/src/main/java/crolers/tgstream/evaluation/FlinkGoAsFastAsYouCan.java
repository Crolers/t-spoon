package crolers.tgstream.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Sample wordcount to evaluate how much flink can go fast
 */
public class FlinkGoAsFastAsYouCan {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final long bufferTimeout = parameters.getLong("bufferTO", 0);
        final int par = parameters.getInt("par", 4);
        final int sourcePar = parameters.getInt("sourcePar", par);
        final int sinkPar = parameters.getInt("sinkPar", par);
        final int batchSize = parameters.getInt("batchSize", 10000);

        env.setBufferTimeout(bufferTimeout);
        env.setParallelism(par);

        DataStream<String> text = env.addSource(new LineSource(batchSize)).setParallelism(sourcePar);

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .keyBy(0).sum(1);
        counts.addSink(new PointlessSink<>()).setParallelism(sinkPar);

        env.execute("Need4Speed");
    }

    private static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    private static class LineSource extends RichParallelSourceFunction<String> {
        private int limit, globalLimit;
        private boolean stop = false;
        private Random random;
        public static final String[] LINES = {
                "Tomorrow, what kind of life do you think of the power of life?",
                "accumsan facilisis mauris eu cursus dolor convallis sed Duis consectetur sem",
                "it will be possible to put it in the free land of the worker, and he flatters the same as in",
                "as long as we live and live a free life, the people of the lacinia tincidunt Proin id pellentesque",
                "a wise person will pursue a career, whether he wishes to adorn his life or his estate",
                "Vulputa, but the urn is wise with arrows, it will be my vestibule sometimes, but there is none.",
                "accumsan urn vitae empdiet semper Quisque at no nibh Maecenas eget massa",
                "It is the people who live in old age, old age, and hungry people.",
                "and the ugly lawlessness of tomorrow sometimes places the elit and the said Morbi to put the dui at",
                "varius lacinia dui sem mollis nisi ac eleifend tortor metus feugiat erat"
        };

        public LineSource(int globalLimit) {
            this.globalLimit = globalLimit;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int numberOfTasks = getRuntimeContext().getNumberOfParallelSubtasks();
            int taskId = getRuntimeContext().getIndexOfThisSubtask();

            this.limit = globalLimit / numberOfTasks;

            if (taskId == 0) {
                this.limit += globalLimit % numberOfTasks;
            }

            this.random = new Random(taskId);
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (!stop && limit > 0) {
                sourceContext.collect(LINES[random.nextInt(LINES.length)]);
                limit--;
            }
        }

        @Override
        public void cancel() {
            stop = true;
        }
    }

    private static class PointlessSink<T> implements SinkFunction<T> {
        private int count = 0;

        @Override
        public void invoke(T o) throws Exception {
            count++;
        }
    }
}

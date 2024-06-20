package crolers.tgstream.common;

import crolers.tgstream.runtime.JobControlClient;
import crolers.tgstream.runtime.JobControlListener;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;

public abstract class ControlledSource<T> extends RichParallelSourceFunction<T> implements JobControlListener {
    protected transient Logger LOG;

    private Semaphore jobFinish = new Semaphore(0);
    protected transient JobControlClient jobControlClient;
    protected volatile boolean stop;
    protected int numberOfTasks, taskId;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.observe(this);

        numberOfTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        taskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    protected void waitForFinish() throws InterruptedException {
        //线程阻塞等待直到 onJobFinish() 调用
        jobFinish.acquire();
    }

    @Override
    public void cancel() {
        stop = true;
    }

    @Override
    public void onJobFinish() {
        JobControlListener.super.onJobFinish();
        stop = true;
        //释放许可
        jobFinish.release();
    }

    @Override
    public void onBatchEnd() {
        // adapter
    }
}

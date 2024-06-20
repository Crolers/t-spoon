package crolers.tgstream.runtime;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 * 1 observer per machine
 */
// 监控 client 的状态（通过解析 server 的 msg）并通知对应的 JobControlListener。JobControlObserver based on Singleton
public class JobControlObserver implements Runnable {
    public static final String finishPattern = "FINISHED";
    public static final String finishedExceptionallyFormat = "FINISHED,%s";
    public static String batchEndPattern = "BATCH_END";

    //----- Start Singleton
    private static JobControlObserver instance;

    private JobControlObserver() {
    }

    public static synchronized JobControlObserver open(
            String jobControlServerAddress, int jobControlServerPort) throws IOException {
        if (instance == null) {
            instance = new JobControlObserver();
            instance.jobControlClient = new JobControlClient(jobControlServerAddress, jobControlServerPort);
            instance.jobControlClient.init();
            instance.LOG = Logger.getLogger(JobControlObserver.class.getSimpleName());

            // subscribe
            instance.jobControlClient.send(JobControlServer.subscribePattern);
            new Thread(instance).start();
        }

        return instance;
    }

    public static synchronized void close() throws IOException {
        if (!instance.jobControlClient.isClosed()) {
            instance.jobControlClient.close();
            instance.stop = true;
        }
    }
    //----- End Singleton

    // JobControlObserver类内嵌了一个JobControlClient类的实例，用于与JobControlServer通信。
    private transient Logger LOG;
    private volatile boolean stop = false;
    private transient JobControlClient jobControlClient;
    private List<JobControlListener> listeners = new LinkedList<>();

    public synchronized void observe(JobControlListener listener) {
        listeners.add(listener);
    }
    //通知消息的处理逻辑则在notifyListeners()方法中实现，该方法接收一个Consumer类型的参数，并将其应用于listeners列表中缓存的所有JobControlListener。
    private synchronized void notifyListeners(Consumer<JobControlListener> logic) {
        for (JobControlListener listener : listeners) {
            logic.accept(listener);
        }
    }

    private void processNotification(String message) {
        if (message.startsWith(finishPattern)) {
            if (message.contains(",")) {
                String exceptionMessage = message.split(",")[1];
                notifyListeners(l -> l.onJobFinishExceptionally(exceptionMessage));
            } else {
                notifyListeners(JobControlListener::onJobFinish);
            }
        } else if (message.equals(batchEndPattern)) {
            notifyListeners(JobControlListener::onBatchEnd);
        } else {
            throw new IllegalArgumentException("Cannot process notification: " + message);
        }
    }

    @Override
    public void run() {
        LOG.info("JobControlObserver listening for notifications at " + jobControlClient.address);

        try {
            while (!stop) {
                String message = jobControlClient.receive();
                if (message == null) {
                    break;
                }

                LOG.info("Received notification " + message);
                processNotification(message);
            }
        } catch (IOException e) {
            LOG.error("Exception while observing: " + e.getMessage());
        }
    }
}

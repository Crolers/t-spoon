package crolers.tgstream.runtime;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class AbstractServer implements Runnable {
    protected final String CLASS_NAME = AbstractServer.this.getClass().getSimpleName();
    protected final Logger LOG = Logger.getLogger(CLASS_NAME);

    private int listenPort;
    private ServerSocket srv;
    private List<ClientHandler> handlers;
    private List<Thread> executors;

    private volatile boolean stop;

    public AbstractServer() {
        this.handlers = new LinkedList<>();
        this.executors = new LinkedList<>();
        this.stop = false;
    }


    protected abstract ClientHandler getHandlerFor(Socket s);

    public void init(int startPort, int endPort) throws IOException {
        if (srv != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        // try to listen port in range of (startPort, endPort)
        for (; startPort <= endPort; startPort++) {
            try {
                srv = new ServerSocket(startPort);
                listenPort = startPort;
                LOG.info(CLASS_NAME + " listening on " + startPort);
                open();
                return;
            } catch (IOException ex) {
                //端口已被占用：如果指定的端口已被其他应用程序占用，则构造方法会抛出 IOException。
                // do nothing, try next port...
                // add random sleep to avoid clash in requests for port
                //多个线程或进程同时尝试创建 ServerSocket，可能会造成竞争条件，导致其中一个线程或进程在创建服务时失败。
                //为了避免这种情况，我们会通过随机休眠一段时间来让其他线程或进程有更多机会去创建 ServerSocket 并占用其他端口，以减少竞争的概率
                try {
                    TimeUnit.MILLISECONDS.sleep(new Random().nextLong() % 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // if the program gets here, no port in the range was found
        throw new IOException("no free port found");
    }

    /**
     * Hook on init. Override for custom behaviour.
     */
    protected void open() throws IOException {
    }

    public void init(int listenPort) throws IOException {
        if (srv != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        srv = new ServerSocket(listenPort);
        this.listenPort = listenPort;
        open();
        LOG.info(CLASS_NAME + " listening on " + listenPort);
    }


    @Override
    public void run() {
        try {
            while (!stop) {
                Socket s = srv.accept();
                LOG.info(String.format("Connection accepted: %s:%d", s.getLocalAddress(), s.getLocalPort()));
                ClientHandler r = getHandlerFor(s);
                handlers.add(r);
                Thread executor = new Thread(r);
                executors.add(executor);
                r.init();
                executor.start();
            }
        } catch (SocketException se) {
            // should happen when the socket is closed
            LOG.info(se.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws Exception {
        this.stop = true;
        srv.close();
        for (ClientHandler r : handlers) {
            r.close();
        }

        /*
        // waiting for termination could cause deadlock...
        for (Thread t : executors) {
            t.join();
        }
        */
    }

    public int getPort() {
        return listenPort;
    }

    public String getIP() throws UnknownHostException {
        return NetUtils.getMyIp();
    }

    public boolean isBound() {
        return srv != null && !srv.isClosed();
    }
}
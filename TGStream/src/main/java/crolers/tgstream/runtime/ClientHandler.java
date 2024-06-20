package crolers.tgstream.runtime;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by crolers
 */
public abstract class ClientHandler implements Runnable {
    protected final Logger LOG = Logger.getLogger(ClientHandler.this.getClass().getSimpleName());

    protected Socket socket;
    protected InputStream in;
    protected OutputStream out;

    public ClientHandler(Socket s) {
        this.socket = s;
    }

    protected void init() throws IOException {
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    protected abstract void lifeCycle() throws Exception;

    @Override
    public void run() {
        try {
            lifeCycle();
        } catch (IOException ioe) {
            LOG.error(ioe.getMessage());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void close() throws IOException {
        socket.close();
        in.close();
        out.close();
    }
}

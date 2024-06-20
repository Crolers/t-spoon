package crolers.tgstream.runtime;

import java.io.IOException;

/**
 * Created by crolers
 * <p>
 * Decorates a ClientHandler by making its lifeCycle loop.
 */
public class LoopingClientHandler extends ClientHandler {
    private boolean stop;
    private ClientHandler handler;

    public LoopingClientHandler(ClientHandler handler) {
        super(handler.socket);
        this.handler = handler;
    }

    @Override
    protected void init() throws IOException {
        super.init();
        handler.init();
    }

    @Override
    protected void lifeCycle() throws Exception {
        while (!stop) {
            handler.lifeCycle();
        }
    }

    protected void stop() {
        stop = true;
    }

    @Override
    public void close() throws IOException {
        super.close();
        handler.close();
        stop();
    }
}

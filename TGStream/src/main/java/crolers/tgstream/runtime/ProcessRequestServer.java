package crolers.tgstream.runtime;

import java.io.IOException;
import java.net.Socket;

public abstract class ProcessRequestServer extends AbstractServer {

    @Override
    public ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(new StringClientHandler(s) {
            @Override
            protected void lifeCycle() throws Exception {
                String request = receive();
                if (request == null) {
                    throw new IOException("Request is null");
                }
                parseRequest(request);
            }
        });
    }

    protected abstract void parseRequest(String request);
}

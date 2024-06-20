package crolers.tgstream.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public abstract class ObjectClientHandler extends ClientHandler {
    protected ObjectInputStream in;
    protected ObjectOutputStream out;
    private int noMessagesSent = 0;

    public ObjectClientHandler(Socket s) {
        super(s);
    }

    @Override
    protected void init() throws IOException {
        super.init();
        this.out = new ObjectOutputStream(super.out);
        out.flush();
        this.in = new ObjectInputStream(super.in);
    }

    protected void send(Object data) throws IOException {
        out.writeUnshared(data);
        noMessagesSent++;

        // prevent memory leak
        if (noMessagesSent % 100000 == 0) {
            out.reset();
        }
    }

    protected Object receive() throws IOException, ClassNotFoundException {
        return in.readUnshared();
    }
}

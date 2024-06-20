package crolers.tgstream.runtime;

import crolers.tgstream.common.Address;

/**
 * Enriched class of AbstractServer
 */
public class WithServer {
    private AbstractServer server;
    private NetUtils.ServerType serverType;
    private Address myAddress;

    public WithServer(AbstractServer server) {
        this(server, null);
    }

    public WithServer(AbstractServer server, NetUtils.ServerType serverType) {
        this.server = server;
        this.serverType = serverType;
    }

    public void open() throws Exception {
        if (serverType == null) {
            server = NetUtils.getServer(NetUtils.MIN_PORT, NetUtils.MAX_PORT, server);
        } else {
            server = NetUtils.getServer(serverType, server);
        }
        myAddress = Address.of(server.getIP(), server.getPort());
    }

    public void close() throws Exception {
        server.close();
    }

    public Address getMyAddress() {
        return myAddress;
    }
}

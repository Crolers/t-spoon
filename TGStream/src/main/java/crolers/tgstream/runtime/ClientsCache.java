package crolers.tgstream.runtime;

import crolers.tgstream.common.Address;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ClientsCache<C extends AbstractClient> {
    private Map<Address, C> clients = new HashMap<>();
    private Function<Address, C> clientSupplier;

    public ClientsCache(Function<Address, C> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    public synchronized C getOrCreateClient(Address address) throws IOException {
        C client = clients.get(address);
        if (client == null) {
            client = clientSupplier.apply(address);
            client.init();
            clients.put(address, client);
        }
        return client;
    }

    public synchronized void clear() throws IOException {
        for (C client : clients.values()) {
            client.close();
        }
        clients.clear();
    }
}

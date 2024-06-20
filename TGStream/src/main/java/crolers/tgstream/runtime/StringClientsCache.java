package crolers.tgstream.runtime;

/**
 * Created by crolers
 */
public class StringClientsCache extends ClientsCache<StringClient> {
    public StringClientsCache() {
        super(a -> new StringClient(a.ip, a.port));
    }
}

package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.StringClientsCache;
import it.polimi.affetti.tspoon.tgraph.Metadata;

import java.util.Collections;

/**
 * Created by affo on 10/11/17.
 */
public class VolatileSinkTransactionCloser implements CloseSinkTransactionCloser {
    private StringClientsCache clients;

    @Override
    public void open() throws Exception {
        this.clients = new StringClientsCache();
    }

    @Override
    public void close() throws Exception {
        clients.clear();
    }

    @Override
    public void onMetadata(Metadata metadata) throws Exception {
        int dependency;

        if (metadata.dependencyTracking.isEmpty()) {
            dependency = -1;
        } else {
            dependency = Collections.max(metadata.dependencyTracking);
        }

        String message = CloseTransactionNotification.serialize(
                metadata.timestamp,
                metadata.vote,
                metadata.cohorts.size(),
                dependency, ""
        );

        clients.getOrCreateClient(metadata.coordinator).send(message);


        for (Address cohort : metadata.cohorts) {
            clients.getOrCreateClient(cohort).send(message);
        }
    }
}

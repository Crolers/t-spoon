package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Created by affo on 09/11/17.
 */
public interface StateOperatorTransactionCloser extends Serializable {
    // For lifecycle
    void open() throws Exception;

    void close() throws Exception;

    void closeTransaction(Address coordinatorAddress, int timestamp, String request,
                          Consumer<Void> success, Consumer<Throwable> error);
}
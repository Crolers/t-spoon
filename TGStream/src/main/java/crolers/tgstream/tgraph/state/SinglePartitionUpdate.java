package crolers.tgstream.tgraph.state;

import crolers.tgstream.common.PartitionOrBcastPartitioner;
import crolers.tgstream.common.RPC;
import crolers.tgstream.evaluation.UniquelyRepresentableForTracking;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

public class SinglePartitionUpdate implements PartitionOrBcastPartitioner.Partitionable<String>,
        UniquelyRepresentableForTracking, Serializable {
    public String nameSpace, key;
    public SinglePartitionUpdateID id;
    public RPC command;

    public SinglePartitionUpdate() {
    }

    public SinglePartitionUpdate(SinglePartitionUpdateID id, String namespace, String key, RPC command) {
        this.nameSpace = namespace;
        this.key = key;
        this.id = id;
        this.command = command;
    }

    @Override
    public Set<String> getKeys() {
        return Collections.singleton(getKey());
    }

    public String getKey() {
        return key;
    }

    public RPC getCommand() {
        return command;
    }

    @Override
    public void setNumberOfPartitions(int n) {
        // does nothing, always 1 partition
    }

    @Override
    public String getUniqueRepresentation() {
        return id.getUniqueRepresentation();
    }

    @Override
    public String toString() {
        return command.getClass().getSimpleName() + "[" + nameSpace + ", " + id + "]";
    }
}

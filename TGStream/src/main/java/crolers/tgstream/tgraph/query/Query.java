package crolers.tgstream.tgraph.query;

import crolers.tgstream.common.PartitionOrBcastPartitioner;
import crolers.tgstream.evaluation.UniquelyRepresentableForTracking;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by crolers
 */
public class Query implements Serializable, UniquelyRepresentableForTracking,
        PartitionOrBcastPartitioner.Partitionable<String> {
    public final QueryID queryID;
    public final String nameSpace;
    public final Set<String> keys = new HashSet<>();
    public long watermark;
    public int numberOfPartitions;
    public QueryResult result;

    public Query(String nameSpace, QueryID queryID) {
        this.queryID = queryID;
        this.nameSpace = nameSpace;
    }

    @Override
    public Set<String> getKeys() {
        return keys;
    }

    @Override
    public void setNumberOfPartitions(int n) {
        this.numberOfPartitions = n;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public QueryResult getResult() {
        if (result == null) {
            result = new QueryResult(getQueryID(), numberOfPartitions);
        }
        return result;
    }

    public QueryID getQueryID() {
        return queryID;
    }

    public void addKey(String key) {
        keys.add(key);
    }

    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "Query{" +
                "nameSpace='" + nameSpace + '\'' +
                ", keys=" + keys +
                ", watermark=" + watermark +
                '}';
    }

    @Override
    public String getUniqueRepresentation() {
        return queryID.getUniqueRepresentation();
    }
}

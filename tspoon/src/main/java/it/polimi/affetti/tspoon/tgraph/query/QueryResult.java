package it.polimi.affetti.tspoon.tgraph.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by affo on 18/12/17.
 */
public class QueryResult implements Serializable {
    public final Map<String, Object> result;

    public QueryResult() {
        this.result = new HashMap<>();
    }

    public void add(String key, Object partialResult) {
        this.result.put(key, partialResult);
    }

    public void merge(QueryResult other) {
        this.result.putAll(other.result);
    }

    public Iterator<Map.Entry<String, Object>> getResult() {
        return result.entrySet().iterator();
    }

    @Override
    public String toString() {
        return result.toString();
    }
}
package crolers.tgstream.tgraph.query;

import crolers.tgstream.common.PartitionOrBcastPartitioner;

import java.io.Serializable;
import java.util.function.Predicate;

public class PredicateQuery<T> extends Query implements PartitionOrBcastPartitioner.Broadcastable {
    public final QueryPredicate<T> predicate;

    public PredicateQuery(String nameSpace, QueryID queryID) {
        this(nameSpace, queryID, new SelectNone<>());
    }

    public PredicateQuery(String nameSpace, QueryID queryID, QueryPredicate<T> predicate) {
        super(nameSpace, queryID);
        this.predicate = predicate;
    }

    public boolean test(T value) {
        return predicate.test(value);
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }

    public interface QueryPredicate<T> extends Predicate<T>, Serializable {
    }

//  Predicate functional interface usage exp:
//    public static void main(String[] args) {
//        List<Integer> numbers = Arrays.asList(-1, -2, 3, 4, -5, 6, -7, 8, 9, -10);
//        List<Integer> positiveNumbers = numbers.stream().filter(isPositive).collect(Collectors.toList()); // 过滤正数
//        System.out.println(positiveNumbers); // [3, 4, 6, 8, 9]
//    }

    public static class SelectAll<T> implements QueryPredicate<T> {

        @Override
        public boolean test(T anObject) {
            return true;
        }
    }

    public static class SelectNone<T> implements QueryPredicate<T> {

        @Override
        public boolean test(T anObject) {
            return false;
        }
    }
}

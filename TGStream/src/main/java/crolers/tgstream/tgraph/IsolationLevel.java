package crolers.tgstream.tgraph;

/**
 * Created by crolers
 */
public enum IsolationLevel {
    PL0, PL1, PL2, PL3, PL4;

    public boolean gte(IsolationLevel other) {
        return this.ordinal() >= other.ordinal();
    }
}

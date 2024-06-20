package crolers.tgstream.tgraph;

/**
 * Created by crolers
 */
public enum Vote {
    COMMIT, ABORT, REPLAY;

    public Vote merge(Vote other) {
        if (this == REPLAY) {
            return REPLAY;
        }

        switch (other) {
            case COMMIT:
                return this;
            default:
                return other;
        }
    }
}

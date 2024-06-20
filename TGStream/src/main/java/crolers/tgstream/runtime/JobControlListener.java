package crolers.tgstream.runtime;

import java.io.IOException;

/**
 * Created by crolers
 */
// listen to JobControlObserver
public interface JobControlListener {
    default void onJobFinish() {
        // every operator must close the observer
        try {
            JobControlObserver.close();
        } catch (IOException e) {
            throw new RuntimeException("Exception while closing JobControlObserver: " + e.getMessage());
        }
    }

    default void onJobFinishExceptionally(String exceptionMessage) {
        // ignore the message by default
        onJobFinish();
    }

    void onBatchEnd();
}

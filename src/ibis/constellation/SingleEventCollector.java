package ibis.constellation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.context.UnitActivityContext;

/**
 * A <code>SingleEventCollector</code> is an {@link Activity} that just waits
 * for a single event, and saves it. It provides a method
 * {@link #waitForEvent()}, to be used by other activities, to collect the event
 * and block until it arrives, after which the <code>SingleEventCollector</code>
 * will finish.
 */
public class SingleEventCollector extends Activity {

    public static final Logger logger = LoggerFactory
            .getLogger(SingleEventCollector.class);

    private static final long serialVersionUID = -538414301465754654L;

    private Event event;

    /**
     * Constructs a <code>SingleEventCollector</code> with the specified
     * activity context. Note: this is an activity that will receive events (see
     * {@link Activity#Activity(ActivityContext, boolean)}).
     *
     * @param c
     *            the activity context of this event collector
     */
    public SingleEventCollector(ActivityContext c) {
        super(c, true, true);
    }

    /**
     * Constructs a <code>SingleEventCollector</code> with the default activity
     * context. Note: this is an activity that will receive events (see
     * {@link Activity#Activity(ActivityContext, boolean)}).
     */
    public SingleEventCollector() {
        this(UnitActivityContext.DEFAULT);
    }

    @Override
    public void initialize() throws Exception {
        suspend();
    }

    @Override
    public synchronized void process(Event e) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("SINGLE EVENT COLLECTOR ( " + identifier()
                    + ") GOT RESULT!");
        }

        event = e;
        notifyAll();
        finish();
    }

    @Override
    public void cleanup() throws Exception {
        // empty
    }

    @Override
    public void cancel() throws Exception {
        // empty
    }

    @Override
    public String toString() {
        return "SingleEventCollector(" + identifier() + ")";
    }

    /**
     * This method blocks waiting for this object to receive an event. As soon
     * as it is available, it returns the event.
     *
     * @return the received event.
     */
    public synchronized Event waitForEvent() {
        while (event == null) {
            try {
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        return event;
    }
}
package ibis.constellation;

/**
 * Main interface to Constellation.
 *
 * @version 1.0
 * @since 1.0
 */
public interface Constellation {

    /**
     * Submit an activity.
     *
     * The submitted Activity will be inserted into Constellation and executed
     * if and when a suitable Executor is found. An ActivityIdentifier is
     * returned that can be used to refer to this submitted Activity at a later
     * moment in time.
     *
     * TODO: can this fail? Describe exceptions TODO: remove? Problem is that an
     * implementation cannot decide which executor is suitable, because although
     * the contexts may match, the steal pool of the executor may be wrong for
     * activities instantiated by this activity.
     *
     * @param job
     *            the Activity to submit
     * @return ActivityIdentifier that can be used to refer to the submitted
     *         Activity.
     */
    public ActivityIdentifier submit(Activity job);

    /**
     * Send an event.
     *
     * TODO: can this fail? Describe exceptions
     *
     * @param e
     *            the Event to send.
     */
    public void send(Event e);

    /**
     * Cancel an Activity.
     *
     * TODO: figure out and describe semantics of this.
     *
     * @param activity
     *            activity to be cancelled
     */
    public void cancel(ActivityIdentifier activity);

    /**
     * Activate this Constellation implementation.
     *
     * Constellation implementations start out in in inactive state when they
     * are created. This allows the application to configure Constellation (for
     * example, by setting up the desired combination of distributed and local
     * constellation implementations.
     *
     * Upon activation, the Constellation implementation will activate all
     * sub-constellations, and activate its own executors, steal pools, event
     * queues, etc.
     *
     * @return if the Constellation was activated.
     */
    public boolean activate();

    /**
     * Terminate Constellation.
     *
     * When terminating all sub-constellations will be terminated. Termination
     * may block until all other running constellation implementations in a Pool
     * have also decided to terminate. When this is the case, the
     * {@link Concluder#conclude()} method is called, allowing the application
     * to run some finalization code of its own.
     *
     * @param concluder
     *            object with a {@link Concluder#conclude()} method
     */
    public void done(Concluder concluder);

    /**
     * Terminate Constellation.
     *
     * When terminating all sub-constellations will be terminated. Termination
     * may block until all other running constellation implementations in a Pool
     * have also decided to terminate.
     */
    public void done();

    /**
     * Returns <code>true</code> if this Constellation instance is the master,
     * <code>false</code> otherwise.
     *
     * @return whether this Constellation instance is the master.
     */
    public boolean isMaster();

    /**
     * Returns a unique identifier for this Constellation instance.
     *
     * This {@link ConstellationIdentifier} can be used to uniquely refer to a
     * running Constellation instance. It is also used as part of an
     * ActivityIdentifer.
     *
     * @return a ConstellationIdentifier that uniquely identifies this
     *         Constellation instance.
     */
    public ConstellationIdentifier identifier();

    public CTimer getTimer(String device, String thread, String action);

    public CTimer getTimer();
}
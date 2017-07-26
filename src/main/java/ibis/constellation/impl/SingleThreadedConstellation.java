package ibis.constellation.impl;

import java.io.BufferedOutputStream;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.AbstractContext;
import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationConfiguration;
import ibis.constellation.ConstellationCreationException;
import ibis.constellation.ConstellationProperties;
import ibis.constellation.Event;
import ibis.constellation.NoSuitableExecutorException;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.impl.util.CircularBuffer;
import ibis.constellation.impl.util.Profiling;
import ibis.constellation.impl.util.SimpleWorkQueue;
import ibis.constellation.impl.util.WorkQueue;
import ibis.constellation.impl.termination.Terminateable;
import ibis.constellation.impl.termination.TerminationDetectedException;
import ibis.constellation.impl.termination.Terminator;

public class SingleThreadedConstellation extends Thread implements Terminateable, Terminator<ExecutorWrapper> {

    private static final Logger logger = LoggerFactory.getLogger(SingleThreadedConstellation.class);

    private final boolean PROFILE_STEALS;

    private final MultiThreadedConstellation parent;

    private final Map<ActivityIdentifierImpl, ConstellationIdentifierImpl> exportedActivities = new ConcurrentHashMap<ActivityIdentifierImpl, ConstellationIdentifierImpl>();
    private final Map<ActivityIdentifierImpl, ConstellationIdentifierImpl> relocatedActivities = new ConcurrentHashMap<ActivityIdentifierImpl, ConstellationIdentifierImpl>();

    private final ExecutorWrapper wrapper;

    public ExecutorWrapper getWrapper() {
        return wrapper;
    }

    // Fresh work that anyone may steal
    private final WorkQueue fresh;

    // Fresh work that can only be stolen by one of my peers
    private final WorkQueue restricted;

    // Work that is stolen from an external constellation. It may be run by me
    // or one of my peers.
    private final WorkQueue stolen;

    // Work that has a context that is not supported by our local executor.
    private final WorkQueue wrongContext;

    // Work that may not leave this machine, but has a context that is not
    // supported by our local executor.
    private final WorkQueue restrictedWrongContext;

    // Work that is relocated. Only our local executor may run it.
    private final CircularBuffer<ActivityRecord> relocated = new CircularBuffer<ActivityRecord>(1);

    // Hashmap allowing quick lookup of the activities in our 4 queues.
    private final HashMap<ActivityIdentifierImpl, ActivityRecord> lookup = new HashMap<ActivityIdentifierImpl, ActivityRecord>();

    private final ConstellationIdentifierImpl identifier;

    private PrintStream out;

    // private final Thread thread;

    private final StealPool myPool;
    private final StealPool stealPool;

    private int rank;

    private boolean active;

    private static class PendingRequests {

        private final ArrayList<EventMessage> deliveredApplicationMessages = new ArrayList<EventMessage>();

        private final HashMap<ConstellationIdentifierImpl, StealRequest> stealRequests = new HashMap<ConstellationIdentifierImpl, StealRequest>();

        @Override
        public String toString() {
            return "QUEUES: " + deliveredApplicationMessages.size() + " " + stealRequests.size();
        }
    }

    private final int stealSize;
    private final int stealDelay;

    private long nextStealDeadline;

    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();

    private boolean done = false;

    private final Profiling profiling;
    private final TimerImpl stealTimer;

    private final boolean ignoreEmptyStealReplies;

    private volatile boolean havePendingRequests = false;

    private boolean seenDone = false;

    private final boolean PRINT_STATISTICS;

    private final boolean PROFILE;

    private long stolenJobs;

    private long stealSuccess;

    private long steals;

    private long remoteStolen;

    SingleThreadedConstellation(final ConstellationConfiguration executor, final ConstellationProperties p)
            throws ConstellationCreationException {
        this(null, executor, p);
    }

    public SingleThreadedConstellation(final MultiThreadedConstellation parent, final ConstellationConfiguration config,
            final ConstellationProperties props) throws ConstellationCreationException {

        if (config == null) {
            throw new IllegalArgumentException("SingleThreadedConstellation expects ConstellationConfiguration");
        }

        if (props == null) {
            throw new IllegalArgumentException("SingleThreadedConstellation expects ConstellationProperties");
        }

        PROFILE_STEALS = props.PROFILE_STEAL;
        PRINT_STATISTICS = props.STATISTICS;
        PROFILE = props.PROFILE;

        logger.info("PROFILE_STEALS = " + PROFILE_STEALS);

        logger.info("PROFILE_STEALS = " + PROFILE_STEALS);

        // this.thread = this;
        this.parent = parent;

        if (parent != null) {
            identifier = parent.getConstellationIdentifierFactory().generateConstellationIdentifier();
        } else {
            // We're on our own
            identifier = new ConstellationIdentifierImpl(0, 0);
        }

        stolen = new SimpleWorkQueue("ST(" + identifier + ")-stolen");
        restricted = new SimpleWorkQueue("ST(" + identifier + ")-restricted");
        fresh = new SimpleWorkQueue("ST(" + identifier + ")-fresh");
        wrongContext = new SimpleWorkQueue("ST(" + identifier + ")-wrong");
        restrictedWrongContext = new SimpleWorkQueue("ST(" + identifier + ")-restrictedwrong");

        super.setName(identifier().toString());

        final String outfile = props.STATISTICS_OUTPUT;

        if (outfile != null) {
            final String filename = outfile + "." + identifier.getNodeId() + "." + identifier.getLocalId();

            try {
                out = new PrintStream(new BufferedOutputStream(new FileOutputStream(filename)));
            } catch (final Throwable e) {
                logger.error("Failed to open output file " + filename);
                out = System.out;
            }

        } else {
            out = System.out;
        }

        if (logger.isInfoEnabled()) {
            logger.info("Starting SingleThreadedConstellation: " + identifier);
        }

        stealDelay = props.STEAL_DELAY;

        if (logger.isInfoEnabled()) {
            logger.info("SingleThreaded: steal delay set to " + stealDelay + " ms.");
        }

        stealSize = props.STEAL_SIZE;

        if (logger.isInfoEnabled()) {
            logger.info("SingleThreaded: steal size set to " + stealSize);
        }

        ignoreEmptyStealReplies = props.STEAL_IGNORE_EMPTY_REPLIES;

        if (logger.isInfoEnabled()) {
            logger.info("SingleThreaded: ignore empty steal replies set to " + ignoreEmptyStealReplies);
        }

        if (parent != null) {
            profiling = parent.getProfiling();
        } else {
            profiling = new Profiling(identifier.toString());
        }

        stealTimer = profiling.getTimer("java", identifier().toString(), "steal");

        wrapper = new ExecutorWrapper(this, props, identifier, config);

        myPool = wrapper.belongsTo();
        stealPool = wrapper.stealsFrom();



    }

    public void setRank(final int rank) {
        this.rank = rank;
    }

    public int getRank() {
        return rank;
    }

    public StealPool belongsTo() {
        return myPool;
    }

    public StealPool stealsFrom() {
        return stealPool;
    }

    public AbstractContext getContext() {
        return wrapper.getContext();
    }

    public StealStrategy getLocalStealStrategy() {
        return wrapper.getLocalStealStrategy();
    }

    public StealStrategy getConstellationStealStrategy() {
        return wrapper.getConstellationStealStrategy();
    }

    public StealStrategy getRemoteStealStrategy() {
        return wrapper.getRemoteStealStrategy();
    }

    public ConstellationIdentifierImpl identifier() {
        return identifier;
    }

    public ActivityIdentifier performSubmit(final Activity activity) throws NoSuitableExecutorException {
        /*
         * This method is called by MultiThreadedConstellation, in case the user
         * calls submit() on a constellation instance. The
         * MultiThreadedConstellation then picks a specific
         * SingleThreadedConstellation, and the activity should be submitted to
         * its wrapper, because this executor may not be able to steal.
         */
        
        
        /** TD: We are given something so start termination detection if not already started */
        terminationDetectionInitiate("  Termination detection started by performSubmit() @ " + identifier);
        
        /** TD: We pass something down. We never had it in our queues so we don't decrease activitiesInQueue */
        return wrapper.submit(activity);
    }

    /**
     * TD: This is only called from below ?
     * 
     * @param ar
     * @param c
     * @param id
     * @return
     */
    public ActivityIdentifierImpl doSubmit(final ActivityRecord ar, final AbstractContext c,
            final ActivityIdentifierImpl id) {

        if (ContextMatch.match(c, wrapper.getContext())) {

            synchronized (this) {
                lookup.put(ar.identifier(), ar);

                if (ar.isRestrictedToLocal()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Submit job to restricted, length was " + restricted.size());
                    }
                    restricted.enqueue(ar);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Submit job to fresh, length was " + fresh.size());
                    }
                    fresh.enqueue(ar);
                }
                
                /** TD: This should probably do nothing because doSubmit() is only called from within the
                 *      wrapper.submit() and this.performSubmit() is called before the wrapper.submit() */
                terminationDetectionInitiate("  Termination detection started by doSubmit() @ " + identifier);
                
                /** TD: We added something to some queue */
                this.incActivities(1);
            }
        } else {
            /** TD: we also add something to some queue but it's handled by deliverWrongContext() itself */
            deliverWrongContext(ar);
        }
        
       

        return id;
    }

    public void performSend(final Event e) {
        logger.error("INTERNAL ERROR: Send not implemented!");
    }

    public void performCancel(final ActivityIdentifier aid) {
        logger.error("INTERNAL ERROR: Cancel not implemented!");
    }

    /**
     * Called by the parent to Activate me
     * 
     * @return
     */
    public boolean performActivate() {
        
        synchronized (this) {
            if (active) {
                return false;
            }

            active = true;
            
            /** TD: Initialize termination detection */
            terminationDetectionInitialize(null);
        }

        start();
        return true;
    }

    public synchronized void performDone() {

        if (!active) {
            return;
        }

        done = true;
        havePendingRequests = true;
        notifyAll();
        if (parent == null) {
            return;
        }
        while (!seenDone) {
            try {
                wait();
            } catch (final InterruptedException e) {
                // ignore
            }
        }
    }
    
    /**
     * Copy the contents of {@code a} into a new array of length {@code count} <br/><br/>
     * Used by attemptStea()
     * 
     * Q: I guess it's because attemptSteal() can return less than *size, so we 
     *    trim the array for it's length to match the number of stolen activities?
     * 
     * @param a
     * @param count
     * @return
     */
    private ActivityRecord[] trim(final ActivityRecord[] a, final int count) {
        if (a.length > count) {
            return Arrays.copyOf(a, count);
        }
        return a;
    }
    
    /**
     * Called by the parent
     * 
     * @param context
     * @param s
     * @param pool
     * @param source
     * @param size
     * @param local
     * @return
     */
    public ActivityRecord[] attemptSteal(final AbstractContext context, final StealStrategy s, final StealPool pool,
            final ConstellationIdentifierImpl source, final int size, final boolean local) {

        final ActivityRecord[] result = new ActivityRecord[size];

        /** TD: we subtract leaving activities inside this attemptSteal() */
        final int count = attemptSteal(result, context, s, pool, source, size, local);

        if (count == 0) {
            return null;
        }
        
        return trim(result, count);
    }
    
    /**
     * TD: Only called from within attemptSteal which handles updating activity count.
     *     Thus, no need to take action here
     *     
     * @param context
     * @param s
     * @param result
     * @param o
     * @param size
     * @return
     */
    private int localSteal(final AbstractContext context, final StealStrategy s, final ActivityRecord[] result,
            final int o, final int size) {
        int offset = o;
        if (offset < size) {
            offset += restrictedWrongContext.steal(context, s, result, offset, size - offset);
        }

        if (offset < size) {
            offset += restricted.steal(context, s, result, offset, size - offset);
        }

        if (offset < size) {
            offset += stolen.steal(context, s, result, offset, size - offset);
        }

        return offset;
    }
    
    /**
     * Our parent calls this to handle a steal request
     * 
     * @param tmp
     * @param context
     * @param s
     * @param pool
     * @param src
     * @param size
     * @param local
     * @return
     */
    public synchronized int attemptSteal(final ActivityRecord[] tmp, final AbstractContext context,
            final StealStrategy s, final StealPool pool, final ConstellationIdentifierImpl src, final int size,
            final boolean local) {

        // attempted steal request from parent. Expects an immediate reply
        steals++;

        // sanity check
        if (src.equals(identifier)) {
            logger.error("INTERAL ERROR: attemp steal from self!", new Throwable());
            return 0;
        }

        if (!pool.overlap(wrapper.belongsTo())) {
            logger.info("attemptSteal: wrong pool!");
            return 0;
        }

        // First steal from the activities that I cannot run myself.
        final int fromWrong = wrongContext.steal(context, s, tmp, 0, size);
        int offset = fromWrong;

        if (local && offset < size) {
            // Only peers from our own constellation are allowed to steal
            // restricted or stolen jobs.
            offset = localSteal(context, s, tmp, offset, size);
        }

        // Anyone may steal a fresh job
        int fromFresh = 0;
        if (offset < size) {
            fromFresh = fresh.steal(context, s, tmp, offset, size - offset);
            offset += fromFresh;
        }

        if (offset == 0) {
            // steal failed, no activities stolen
            return 0;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Stole " + offset + " jobs from " + identifier + ": " + fromWrong + " from wrongContext, "
                    + fromFresh + " from fresh");
        }

        // Next, remove activities from lookup, and mark and register them as
        // relocated or stolen/exported
        registerLeavingActivities(tmp, offset, src, local);
        
        System.out.println("  attemptSteal() " + offset); 
        
        stolenJobs += offset;
        stealSuccess++;
        
        /** TD: We removed *offset* activities from our queues */
        this.decActivities(offset);

        return offset;
    }

    private synchronized void registerLeavingActivities(final ActivityRecord[] ar, final int len,
            final ConstellationIdentifierImpl dest, final boolean isLocal) {

        for (int i = 0; i < len; i++) {
            if (ar[i] != null) {
                lookup.remove(ar[i].identifier());

                if (isLocal) {
                    ar[i].setRelocated(true);
                    relocatedActivities.put(ar[i].identifier(), dest);
                } else {
                    ar[i].setStolen(true);
                    exportedActivities.put(ar[i].identifier(), dest);
                }
            }
        }
    }

    public void deliverStealRequest(final StealRequest sr) {
        // steal request (possibly remote) to enqueue and handle later
        if (logger.isTraceEnabled()) {
            logger.trace("S REMOTE STEAL REQUEST from " + sr.source + " context " + sr.context);
        }
        postStealRequest(sr);
    }

    private synchronized boolean pushWorkFromQueue(final WorkQueue queue, final StealStrategy s) {
        if (queue.size() > 0) {
            
            final ActivityRecord ar = queue.steal(wrapper.getContext(), s);
            
            if (ar != null) {
                lookup.remove(ar.identifier());
                wrapper.addPrivateActivity(ar);
                
                /** TD: we remove something from the queue */
                this.decActivities(1);
                
                /** TD: we may have to check for initiating termination (wait the wrapper) here */
                return true;
            }
        }
        return false;
    }

    private synchronized boolean pushRelocatedToExecutor() {
        // Push all relocated activities to our executor.
        if (relocated.size() > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Found work on relocated list: " + relocated.size() + " jobs");
            }
            
            while (relocated.size() > 0) {
                final ActivityRecord ar = relocated.removeFirst();
                lookup.remove(ar.identifier());
                wrapper.addPrivateActivity(ar);
                
                /** TD: we remove something from the relocated queue */
                this.decActivities(1);
                
                /** TD: we may have to check for initiating termination (wait the wrapper) here */
            }

            return true;
        }
        return false;
    }

    /**
     * TD: The activities we remove are handled inside the 
     *     pushRelocatedToExecutor() and pushWorkFromQueue() above
     *     
     * @param s
     * @return
     */
    private synchronized boolean pushWorkToExecutor(final StealStrategy s) {
        
        if (pushRelocatedToExecutor()) {
            System.out.println(this.identifier() + " Push Relocated");
            return true;
        }

        // Else: try to push one restricted activity to our executor
        if (pushWorkFromQueue(restricted, s)) {
            System.out.println(this.identifier() + " Push Restricted");
            return true;
        }

        // Else: try to push one stolen activity to our executor
        if (pushWorkFromQueue(stolen, s)) {
            System.out.println(this.identifier() + " Push Stolen");
            return true;
        }

        // Else: try to push one fresh activity to our executor
        if (pushWorkFromQueue(fresh, s)) {
            System.out.println(this.identifier() + " Push Fresh");
            return true;
        }

        return false;
    }

    public void deliverStealReply(final StealReply sr) {

        if (sr.isEmpty()) {
            // ignore empty replies
            return;
        }

        // If we get a non-empty steal reply, we simply enqueue it locally.
        final ActivityRecord[] tmp = sr.getWork();

        remoteStolen += tmp.length;

        synchronized (this) {

            for (final ActivityRecord a : tmp) {
                if (a != null) {
                    // two options here: either the job is stolen (from a remote
                    // constellation) or
                    // relocated (from a peer in our local constellation).
                    // Stolen jobs may be
                    // relocated later, but relocated jobs must be executed by
                    // this executor.

                    // Timo: Add it to lookup as well!
                    lookup.put(a.identifier(), a);
                    if (a.isRelocated()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Putting " + a.identifier().toString() + " on relocated list of "
                                    + this.identifier().toString());
                        }
                        relocated.insertLast(a);
                    } else {
                        stolen.enqueue(a);
                    }
                    
                    /** TD: we added something */
                    this.incActivities(1);
                    
                    signal();
                }
            }
        }
    }

    public synchronized ConstellationIdentifierImpl deliverEventMessage(final EventMessage m) {
        // A message from above. The target must be local (in one of my queues,
        // or in the queues of the executor) or its new location must be known
        // locally.
        //
        // POSSIBLE RACE CONDITIONS:
        //
        // 1) When the message overtakes a steal request, it may arrive before
        // the activity has arrived. As a result, the activity cannot be found
        // here yet.
        //
        // 2) The activity may be registered in relocated or exported but
        // a) it may not have arrived at the destination yet
        // b) it may about to be reclaimed because the target could not be
        // reached
        //
        // When the message can be delivered, null is returned. When not, the
        // constellation identifier where it should be sent instead is returned.

        final Event e = m.event;
        final ActivityIdentifierImpl target = (ActivityIdentifierImpl) e.getTarget();

        final ActivityRecord tmp = lookup.get(target);

        if (tmp != null) {
            // We found the destination activity and enqueue the event for it.
            tmp.enqueue(e);
            return null;
        }

        // If not, it may have been relocated
        ConstellationIdentifierImpl cid = relocatedActivities.get(target);

        if (cid != null) {
            return cid;
        }

        // If not, it may have been stolen
        cid = exportedActivities.get(target);

        if (cid != null) {
            return cid;
        }

        // If not, it should be in the queue of my executor
        postEventMessage(m);
        return null;
    }

    public boolean isMaster() {
        return parent == null;
    }

    public void handleEvent(final Event e) {
        // An event pushed up by our executor. We know the
        // executor itself does not contain the target activity

        ConstellationIdentifierImpl cid = null;

        final ActivityIdentifierImpl target = (ActivityIdentifierImpl) e.getTarget();

        synchronized (this) {

            // See if the activity is in one of our queues
            final ActivityRecord tmp = lookup.get(e.getTarget());

            if (tmp != null) {
                // It is, so enqueue it and return.
                tmp.enqueue(e);
                return;
            }

            // See if we have exported it somewhere
            cid = exportedActivities.get(target);

            if (cid == null) {
                // If not, it may have been relocated
                cid = relocatedActivities.get(target);
            }

            if (cid == null) {
                // If not, we simply send the event to the parent
                cid = target.getOrigin();
            }
        }

        if (cid.equals(identifier)) {
            // the target is local, which means we have lost a local activity
            logger.error("Activity " + e.getTarget() + " does no longer exist! (event dropped)");
            return;
        }

        parent.handleEventMessage(new EventMessage(identifier, cid, e));
    }

    public synchronized final void signal() {
        havePendingRequests = true;
        notifyAll();
    }

    private synchronized void postStealRequest(final StealRequest s) {

        // sanity check
        if (s.source.equals(identifier)) {
            logger.error("INTERAL ERROR: posted steal request from self!", new Throwable());
            return;
        }

        if (logger.isTraceEnabled()) {
            final StealRequest tmp = incoming.stealRequests.get(s.source);

            if (tmp != null) {
                logger.trace("Steal request overtaken: " + s.source);
            }
        }

        incoming.stealRequests.put(s.source, s);

        signal();
    }

    private synchronized void postEventMessage(final EventMessage m) {
        incoming.deliveredApplicationMessages.add(m);
        signal();
    }

    private synchronized boolean getDone() {
        if (done) {
            seenDone = true;
            notifyAll();
            return true;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("getDone returns false");
        }
        return false;
    }

    private synchronized void swapEventQueues() {

        if (logger.isTraceEnabled()) {
            logger.trace("Processing events while idle!\n" + incoming.toString() + "\n" + processing.toString());
        }

        final PendingRequests tmp = incoming;
        incoming = processing;
        processing = tmp;
        // NOTE: havePendingRequests needs to be set here to prevent a gap
        // between doing the swap + setting it to false. Another submit
        // could potentially use this gap to insert a new event. This would
        // lead to a race condition!
        // But it is probably better to only reset it if done is not set?
        havePendingRequests = done;
    }

    private void processRemoteMessages() {
        for (final EventMessage m : processing.deliveredApplicationMessages) {
            if (!wrapper.queueEvent(m.event)) {
                // Failed to deliver event locally. Check if the activity is
                // now in one of the local queues. If not, return to parent.
                if (logger.isInfoEnabled()) {
                    logger.info("Failed to deliver message from " + m.source + " / " + m.event.getSource() + " to "
                            + m.target + " / " + m.event.getTarget() + " (resending)");
                }

                handleEvent(m.event);
            }
        }
        processing.deliveredApplicationMessages.clear();
    }

    /**
     * Reclaim is used to re-insert activities into the queue whenever a steal
     * reply failed to be sent.
     *
     * @param a
     *            the ActivityRecords to reclaim
     */
    public void reclaim(final ActivityRecord[] a) {

        if (a == null) {
            return;
        }

        for (final ActivityRecord ar : a) {

            if (ar != null) {

                final AbstractContext c = ar.getContext();

                if (ar.isRelocated()) {
                    // We should unset the relocation flag if an activity is
                    // returned.
                    ar.setRelocated(false);
                    
                    /** Q: should this be the relocatedActivities hashmap instead? 
                     *     because in the else case below we remove from exportedActivities,
                     *     and we also add back to some queue in the if below */
                    relocated.remove(ar); 
                    
                    /** TD: we remove from relocated */
                    this.decActivities(1);
                    
                } else if (ar.isStolen()) {
                    // We should unset the stolen flag if an activity is
                    // returned.
                    ar.setStolen(false);
                    exportedActivities.remove(ar.identifier());
                    
                }

                if (ContextMatch.match(c, wrapper.getContext())) {

                    synchronized (this) {
                        lookup.put(ar.identifier(), ar);

                        if (ar.isRestrictedToLocal()) {
                            restricted.enqueue(ar);
                        } else if (ar.isStolen()) {
                            stolen.enqueue(ar);
                        } else {
                            fresh.enqueue(ar);
                        }
                        
                        /** TD: we insert something to some queue */
                        this.incActivities(1);
                    }
                } else {
                    /** TD: we also add something to some queue but it's handled by deliverWrongContext() itself */
                    deliverWrongContext(ar);
                }
            }
        }
    }

    private void processStealRequests() {
        
        final Collection<StealRequest> requests = processing.stealRequests.values();

        for (final StealRequest s : requests) {

            ActivityRecord[] a = null;

            synchronized (this) {

                // We grab the lock here to prevent other threads (from above)
                // from doing a lookup in the
                // relocated/exported tables while we are removing activities
                // from the executor's queue.

                final StealStrategy tmp = s.isLocal() ? s.constellationStrategy : s.remoteStrategy;

                // NOTE: a is allowed to be null
                
                /** TD: we try to steal from the wrapper */
                a = wrapper.steal(s.context, tmp, s.isLocal(), s.size, s.source);

                if (a != null) {
                    // We have a result. Register the leaving activities.
                    registerLeavingActivities(a, a.length, s.source, s.isLocal());
                }
            }

            if (a != null) {
                if (!parent.handleStealReply(this,
                        new StealReply(wrapper.identifier(), s.source, s.pool, s.context, a))) {
                    
                    /** TD: we handle it inside reclaim() */
                    reclaim(a);
                }
            } else if (!ignoreEmptyStealReplies) {
                // No result, but we send a reply anyway.
                parent.handleStealReply(this, new StealReply(wrapper.identifier(), s.source, s.pool, s.context, a));
            } else {
                // No result, and we're not supposed to tell anyone
                if (logger.isDebugEnabled()) {
                    logger.debug("IGNORING empty steal reply");
                }
            }
        }
        processing.stealRequests.clear();
    }

    private void processEvents() {
        swapEventQueues();
        processRemoteMessages();
        processStealRequests();
    }

    private synchronized boolean pauseUntil(final long deadline) {

        long pauseTime = deadline - System.currentTimeMillis();

        while (pauseTime > 0 && !havePendingRequests) {
            try {
                wait(pauseTime);
            } catch (final Throwable e) {
                // ignored
            }
            if (!havePendingRequests) {
                pauseTime = deadline - System.currentTimeMillis();
            } else {
                pauseTime = 0;
            }
        }

        return havePendingRequests;
    }

    private long stealAllowed() {

        if (stealDelay > 0) {

            final long now = System.currentTimeMillis();

            if (logger.isDebugEnabled()) {
                logger.debug("nextStealDeadline - now = " + (nextStealDeadline - now));
            }
            if (now >= nextStealDeadline) {
                nextStealDeadline = now + stealDelay;
                return 0;
            }

            return nextStealDeadline;
        } else {
            return 0;
        }
    }

    private void resetStealDeadline() {
        logger.debug("Resetting steal deadline");
        nextStealDeadline = 0;
    }

    /**
     * TD: This call all increases the total activity count of this instance
     * 
     * @param a
     */
    public synchronized void deliverWrongContext(final ActivityRecord a) {
        // Timo: we should add it to the lookup as well
        lookup.put(a.identifier(), a);

        if (a.isRestrictedToLocal()) {
            restrictedWrongContext.enqueue(a);
            if (logger.isDebugEnabled()) {
                logger.debug("Added job to restrictedWrongContext queue; length = " + restrictedWrongContext.size());
            }
        } else {
            wrongContext.enqueue(a);
            if (logger.isDebugEnabled()) {
                logger.debug("Added job to wrongContext queue; length = " + wrongContext.size());
            }
        }
        
        if ( !terminationDetectionStarted )
            terminationDetectionInitiate("  Termination Detection started by deliverWrongContext() @ " + identifier);
        
        /** TD: We add something in some queue */
        this.activitiesInQueue.incrementAndGet();

        System.out.println("  deliverWrongContext() " + this.activitiesInQueue);
    }

    private synchronized void waitForRequest() {
        while (!havePendingRequests) {
            try {
                wait();
            } catch (final Throwable e) {
                // ignore
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Woke up in processActivities");
            }
        }
    }

    // An Activity.processActivities call ultimately ends up here.
    // We should make progress on each call, either by processing requests, or
    // by doing work.
    // Either that, or we should sleep for a while.
    public boolean processActivities() {
        
        synchronized(System.out) {
            System.out.println("  processActivities(" + identifier +") " + "F: " + fresh.size() + " R: " 
                    + restricted.size() + " S: " + stolen.size() + " W: " + wrongContext.size() + " RWC: " + restrictedWrongContext.size() + " REL: " + relocated.size() );
            
        }
        
        boolean haveRequests = false;
        synchronized (this) {
            if (havePendingRequests) {
                if (getDone()) {
                    return true;
                }
                haveRequests = true;
            }
        }
        if (haveRequests) {
            processEvents();
        }

        boolean wrapperProcess = false;

        try {
            /**TD: the very first time this will return false i guess */
            wrapperProcess = wrapper.process(); // try execute an activity
        } catch (TerminationDetectedException e) {
            /**TD: if all goes well we have termination here (?) */
            if (terminationDetectionStarted) {
                
                System.out.println("  wrapper.process() threw Termination exception! " + identifier);
                
                performTermination();
            }
                
        }

        boolean pushWorkToExecutor = false;

        if (!wrapperProcess) {
            
            /** TD: Any activities that may be passed down are handled in the methods called by pushWorkToExecutor() */
            pushWorkToExecutor = pushWorkToExecutor(wrapper.getLocalStealStrategy()); // try
                                                                                      // push
                                                                                      // something
                                                                                      // down
        }

        if (pushWorkToExecutor) { 
            /** TD: We push something down so we have to wait for the wrapper */
            terminationDetectionInitiate("  termination detection started (by processActivities() > pushWorkToExecutor) " + identifier);
        }

        if (wrapperProcess || pushWorkToExecutor) {
            // Either we processed an activity, or we pushed one to the wrapper.
            return false;
        }

        if (parent == null || stealsFrom() == StealPool.NONE) {
            // Cannot steal, either because there is no-one to steal from, or
            // because of the NONE stealpool.
            waitForRequest();
            return getDone();
        }

        final long nextDeadline = stealAllowed();

        if (nextDeadline == 0) {
            stealFromParent();
        } else {
            pauseUntil(nextDeadline);
        }

        return false;
    }

    private void stealFromParent() {

        int evnt = 0;
        if (PROFILE_STEALS) {
            evnt = stealTimer.start();
        }
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("GENERATING STEAL REQUEST at " + identifier + " with context " + getContext());
            }
            
            /** TD: We immediately pass those to the wrapper so no need to subtract */
            final ActivityRecord[] result = parent.handleStealRequest(this, stealSize);

            if (result != null) { // parent returned something
                boolean more = false;
                for (final ActivityRecord element : result) {
                    if (element != null) {
                        
                        
                        terminationDetectionInitiate("  termination detection started by stealFromParent() @ " + identifier);
                        
                        
                        /** TD: These come from our parent. Don't subtract from the count */
                        wrapper.addPrivateActivity(element);
                        
                        more = true;
                    }
                }
                if (more) {
                    // ignore steal deadline when we are successful!
                    resetStealDeadline();
                }
            }

        } finally {
            if (PROFILE_STEALS) {
                stealTimer.stop(evnt);
            }
        }
    }

    @Override
    public void run() {

        final long start = System.currentTimeMillis();

        wrapper.runExecutor();

        if (PRINT_STATISTICS) {
            printStatistics(System.currentTimeMillis() - start);
        }
    }

    public void printStatistics(final long totalTime) {

        final long messagesInternal = wrapper.getMessagesInternal();
        final long messagesExternal = wrapper.getMessagesExternal();
        final double messagesTime = wrapper.getMessagesTimer().totalTimeVal() / 1000.0;

        final long activitiesSubmitted = wrapper.getActivitiesSubmitted();

        final long wrongContextSubmitted = wrapper.getWrongContextSubmitted();

        final long steals = wrapper.getSteals() + this.steals;
        final long stealSuccessIn = wrapper.getStealSuccess() + this.stealSuccess;
        final long stolen = wrapper.getStolen() + this.stolenJobs;

        final double idleTime = stealTimer.totalTimeVal() / 1000.0;

        final double idlePerc = (100.0 * idleTime) / totalTime;
        final double messPerc = (100.0 * messagesTime) / totalTime;

        final double initializeTime = wrapper.getInitializeTimer().totalTimeVal() / 1000.0;
        final int activitiesInvoked = wrapper.getInitializeTimer().nrTimes();
        final double processTime = wrapper.getProcessTimer().totalTimeVal() / 1000.0;
        final double cleanupTime = wrapper.getCleanupTimer().totalTimeVal() / 1000.0;

        final double fact = ((double) activitiesInvoked) / (activitiesSubmitted);
        final double initializePerc = (100.0 * initializeTime) / totalTime;
        final double processPerc = (100.0 * processTime) / totalTime;
        final double cleanupPerc = (100.0 * cleanupTime) / totalTime;

        synchronized (out) {

            out.println(identifier + " statistics");
            out.println(" Time");
            out.println("   total           : " + totalTime + " ms.");
            if (PROFILE) {
                out.println("   initialize      : " + initializeTime + " ms. (" + initializePerc + " %)");
                out.println("     process       : " + processTime + " ms. (" + processPerc + " %)");
                out.println("   cleanup         : " + cleanupTime + " ms. (" + cleanupPerc + " %)");
                out.println("   message time    : " + messagesTime + " ms. (" + messPerc + " %)");
            }

            if (PROFILE_STEALS) {
                out.println("   idle count      : " + stealTimer.nrTimes());
                out.println("   idle time       : " + idleTime + " ms. (" + idlePerc + " %)");
            }

            out.println(" Activities");
            out.println("   submitted       : " + activitiesSubmitted);
            if (PROFILE) {
                out.println("   invoked         : " + activitiesInvoked + " (" + fact + " /act)");
            }
            out.println("  Wrong Context");
            out.println("   submitted       : " + wrongContextSubmitted);
            out.println(" Messages");
            out.println("   internal        : " + messagesInternal);
            out.println("   external        : " + messagesExternal);
            out.println(" Steals");
            out.println("   incoming        : " + steals);
            out.println("   success         : " + stealSuccessIn);
            out.println("   stolenFromMe    : " + stolen);
            out.println("   stolenfromRemote: " + remoteStolen);
        }

        out.flush();
    }

    public TimerImpl getTimer(final String standardDevice, final String standardThread, final String standardAction) {
        return profiling.getTimer(standardDevice, standardThread, standardAction);
    }

    public Constellation getConstellation() {
        return wrapper;
    }

    public Profiling getProfiling() {
        return profiling;
    }

    /** TERMINATION STUFF */

    private boolean terminationDetectionStarted;

    private AtomicInteger activitiesInQueue;

    private boolean waitingWrapper;
    
    
    /**
     * Initialize
     * 
     * @param msg
     */
    private void terminationDetectionInitialize(String msg) {
        this.terminationDetectionStarted = false;
        this.activitiesInQueue = new AtomicInteger(0);
        this.waitingWrapper = false;
        
        if ( msg != null ) {
            System.out.println(msg);
        } else {
            logger.warn("Initializing (worker) termination detection at " + identifier);
        }
    }
    
    
    /**
     * Start a termination detection procedure if not started already   <br/><br/>
     * 
     * Calling this method means that we are waiting for the wrapper    <br/>
     * to announce and we cannot announce ourselves before this happens <br/><br/>
     * 
     * We could have that {@link #terminationDetectionStarted} == {@code true}   <br/>
     * and {@link #waitingWrapper} == {@code false}. This means that the wrapper <br/>
     * is empty but we still have stuff in our queues.                           <br/><br/>
     * 
     * Q: Can the above happen anyway?
     * 
     * @param msg
     */
    private void terminationDetectionInitiate(String msg) {
        
        if ( terminationDetectionStarted ) {
            logger.warn("termination detection already started (" + identifier + ")");
        } else {
            this.terminationDetectionStarted = true;
            
        }

        if ( waitingWrapper ) {
            logger.warn("Already waiting wrapper!");
        } else {
            this.waitingWrapper = true;
        }
        
        
    }
    
    private void incActivities(int by) {
        
        /** somewhat expensive atomic update, make we have some change from it */
        
        if ( by != 0 ) {
            this.activitiesInQueue.addAndGet(by);
            
            if ( !this.terminationDetectionStarted )
                terminationDetectionStarted = true;
        }

    }
    
    private void decActivities(int by) {
        
        /** somewhat expensive atomic update, make we have some change from it */
        
        if ( by != 0 ) {
            activitiesInQueue.addAndGet(-by);
            
            if ( terminationCheck() )
                performTermination();
        }
    }
    
    private synchronized boolean terminationCheck() {
        if ( !this.terminationDetectionStarted )
            logger.warn("Attempt to check for termination before termination detection started" );
        
        if ( waitingWrapper ) return false;
        
        return activitiesInQueue.get() == 0;
    }
    

    @Override
    public void performTermination() {
        if (!terminationDetectionStarted) {
            /* sanity check */
            logger.warn("Attempt to performTermination() while termination detection is not started "); 
        } else {
            
            /** 1. perform anything else required for termination of this worker here */
            terminationDetectionStarted = false;
            waitingWrapper = false;
            
            /** 2. announce the termination */
            announceTermination();
        }
    }

    @Override
    public synchronized void announceTermination() {
        // TODO Auto-generated method stub
        System.out.println(this.identifier() + " announcing termination to parent " + parent.identifier());
        parent.informTerminated(this);

    }

    @Override
    public void informTerminated(ExecutorWrapper t) {
        
        Objects.requireNonNull(t);
        
        if (t != this.wrapper) {
            /* sanity check */
            System.out.println("Invalid Wrapper for this SingleThreadedConstellation instance");
        } else {
            
            this.waitingWrapper = false;
            
            if(terminationCheck())
                announceTermination();
        }
    }


    

    
    
}

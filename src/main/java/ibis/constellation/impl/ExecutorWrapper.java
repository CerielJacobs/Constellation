package ibis.constellation.impl;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.AbstractContext;
import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationConfiguration;
import ibis.constellation.ConstellationCreationException;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.ConstellationProperties;
import ibis.constellation.Event;
import ibis.constellation.NoSuitableExecutorException;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.impl.termination.*;
import ibis.constellation.impl.util.CircularBuffer;
import ibis.constellation.impl.util.SimpleWorkQueue;
import ibis.constellation.impl.util.WorkQueue;

public class ExecutorWrapper implements Constellation, Terminateable {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorWrapper.class);

    private final boolean PROFILE;
    private final boolean PROFILE_COMM;

    private final int QUEUED_JOB_LIMIT;

    private final SingleThreadedConstellation parent;

    private final ConstellationIdentifierImpl identifier;

    private final AbstractContext myContext;

    private final StealStrategy localStealStrategy;
    private final StealStrategy constellationStealStrategy;
    private final StealStrategy remoteStealStrategy;

    private final StealPool myPool;
    private final StealPool stealsFrom;

    private HashMap<ActivityIdentifier, ActivityRecord> lookup = new HashMap<ActivityIdentifier, ActivityRecord>();

    private final WorkQueue restricted;
    private final WorkQueue fresh;

    private CircularBuffer<ActivityRecord> runnable = new CircularBuffer<ActivityRecord>(1);
    private CircularBuffer<ActivityRecord> relocated = new CircularBuffer<ActivityRecord>(1);

    private long activityCounter = 0;

    private final TimerImpl initializeTimer;
    private final TimerImpl cleanupTimer;
    private final TimerImpl processTimer;

    private long activitiesSubmitted;
    private long wrongContextSubmitted;

    private long steals;
    private long stealSuccess;
    private long stolenJobs;

    private long messagesInternal;
    private long messagesExternal;
    private final TimerImpl messagesTimer;

    ExecutorWrapper(SingleThreadedConstellation parent, ConstellationProperties p, ConstellationIdentifierImpl identifier,
            ConstellationConfiguration config) throws ConstellationCreationException {

        this.parent = parent;
        this.identifier = identifier;
        this.myContext = config.getContext();

        this.myPool = config.getBelongsToPool();
        this.stealsFrom = config.getStealsFrom();
        this.localStealStrategy = config.getLocalStealStrategy();
        this.constellationStealStrategy = config.getConstellationStealStrategy();
        this.remoteStealStrategy = config.getRemoteStealStrategy();

        QUEUED_JOB_LIMIT = p.QUEUED_JOB_LIMIT;

        PROFILE = p.PROFILE;
        PROFILE_COMM = p.PROFILE_COMMUNICATION;

        if (logger.isInfoEnabled()) {
            logger.info("Executor set job limit to " + QUEUED_JOB_LIMIT);
        }
        
        
        /** Q: Are these are different than the SingleThreadedConstellation WorkQueues? */
        restricted = new SimpleWorkQueue("ExecutorWrapper(" + identifier + ")-restricted");
        fresh = new SimpleWorkQueue("ExecutorWrapper(" + identifier + ")-fresh");

        messagesTimer = parent.getTimer("java", parent.identifier().toString(), "message sending");
        initializeTimer = parent.getTimer("java", parent.identifier().toString(), "initialize");
        cleanupTimer = parent.getTimer("java", parent.identifier().toString(), "cleanup");
        processTimer = parent.getTimer("java", parent.identifier().toString(), "process");
        
        this.terminationDetectionInitialize();

    }
    
    /**
     * Remove a runnable Activity
     * 
     * @param activityIdentifier
     * @return 
     *      {@code true}  - An Activity is removed from the queue <br/>
     *      {@code false} - Nothing is removed
     */
    private boolean cancel(ActivityIdentifier activityIdentifier) {
        
        /** Q: Is it possible that lookup.remove() returns non-null
         *     but ar.needsToRun() == false? */
        ActivityRecord ar = lookup.remove(activityIdentifier);

        if (ar == null) {
            return false;
        }

        if (ar.needsToRun()) {
            
            if (runnable.remove(ar)) {
                
                /** TD: we remove one from runnable */
                decActivities(1);
                
                return true;
            }
                
            
        }
        
        return false;
    }

    @Override
    public void done() {
        System.out.println(this.identifier + " Somebody called done()");
        if (lookup.size() > 0) {
            logger.warn("Quiting Constellation with " + lookup.size() + " activities in queue");
        }
        parent.performDone();
    }
    
    /** 
     * Attempt to dequeue an activity
     * 
     * TD:  When this method returns non-null we have removed something
     *      However, we cannot check for termination here because it is
     *      only within from process() and that may dequeue an activity
     *      and run it! Thus we check for termination in process()
     * 
     * @return A dequeued Activity on success or null
     */
    private ActivityRecord dequeue() {

        // Try to dequeue an activity that we can run.

        // First see if any suspended activities have woken up.
        int size = runnable.size();

        if (size > 0) {
            return runnable.removeFirst();
        }

        // Next see if we have any relocated activities.
        size = relocated.size();

        if (size > 0) {
            return relocated.removeFirst();
        }

        // Next see if there are any activities that cannot
        // leave this constellation
        size = restricted.size();

        if (size > 0) {
            return restricted.steal(myContext, localStealStrategy);
        }

        // Finally, see if there are any fresh activities.
        size = fresh.size();

        if (size > 0) {
            return fresh.steal(myContext, localStealStrategy);
        }
        
        
        return null;
    }

    public void addPrivateActivity(ActivityRecord a) {
        // add an activity that only I am allowed to run, either because
        // it is relocated, or because we have just obtained it and we don't
        // want anyone else to steal it from us.
        System.out.println("    addPrivateActivity() " + identifier + " " + this.activitiesInQueue + 1);
        
        lookup.put(a.identifier(), a);
        relocated.insertLast(a);
        
        /** TD: we added to the relocated */
        incActivities(1);
    }

    private synchronized ActivityIdentifierImpl createActivityID(boolean events) {
        return ActivityIdentifierImpl.createActivityIdentifier(identifier, activityCounter++, events);
    }

    @Override
    public ActivityIdentifier submit(Activity activity) throws NoSuitableExecutorException {
        // Create an activity identifier and initialize the activity with it.
        ActivityIdentifierImpl id = createActivityID(activity.expectsEvents());
        activity.setIdentifier(id);

        ActivityRecord ar = new ActivityRecord(activity, id);

        boolean match = ContextMatch.match(myContext, activity.getContext());

        activitiesSubmitted++;

        // First deal with submissions that don't match with my context.
        if (!match) {
            if (parent == null) {
                throw new NoSuitableExecutorException("Cannot execute on this constellation");
            }
            wrongContextSubmitted++;
            
            /** TD: The activity was not in some local queue so we don't decrease activitiesInQueue */
            parent.deliverWrongContext(ar);
            
            return id;
        }

        activitiesSubmitted++;

        if (restricted.size() + fresh.size() >= QUEUED_JOB_LIMIT && !ar.isRestrictedToLocal()) {
            // If we have too much work on our hands we push it to our
            // parent. Added bonus is that others can access it without
            // interrupting me.
            // But we keep restricted jobs anyway, if we can execute them. We might be the only executor that can execute them,
            // and maybe we cannot steal ... --Ceriel
            
            /** TD: The activity was not in some local queue so we don't decrease activitiesInQueue */
            return parent.doSubmit(ar, activity.getContext(), id);
        }

        lookup.put(id, ar);
        
        /** TD: Here we enqueue something so our count is increased */
        incActivities(1);
        
        if (ar.isRestrictedToLocal()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Submit job to restricted of " + identifier + ", length was " + restricted.size());
            }
            restricted.enqueue(ar);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Submit job to fresh of " + identifier + ", length was " + fresh.size());
            }
            fresh.enqueue(ar);
        }
        // Expensive call, but otherwise parent may not see that there
        // is work to do ... this is really only needed when the submit
        // is called from the main program, not if it is called from the
        // activity. But testing for that may be expensive as well.
        parent.signal();

        return id;
    }

    @Override
    public void send(Event e) {

        ActivityIdentifier target = e.getTarget();
        ActivityIdentifier source = e.getSource();
        int evt = 0;

        if (logger.isDebugEnabled()) {
            logger.debug("SEND EVENT " + source + " to " + target);
        }

        if (PROFILE_COMM) {
            evt = messagesTimer.start();
        }

        // First check if the activity is local.
        ActivityRecord ar;

        ar = lookup.get(target);
        if (ar != null) {
            messagesInternal++;
        } else {
            messagesExternal++;
        }

        if (ar != null) {
            ar.enqueue(e);

            boolean change = ar.setRunnable();

            if (change) {
                /** Q: Why do we insert here? This activity is not dequeued anywhere 
                 *     Where does ar come from? Does this mean that queues may have duplicate activities? */
                runnable.insertLast(ar);
            }

        } else {
            // Activity is not local, so let our parent handle it.
            parent.handleEvent(e);
        }

        if (PROFILE_COMM) {
            messagesTimer.stop(evt);
        }
    }

    public boolean queueEvent(Event e) {

        ActivityRecord ar = lookup.get(e.getTarget());

        if (ar != null) {

            ar.enqueue(e);

            boolean change = ar.setRunnable();

            if (change) {
                /** Q: Same as in send() above */
                runnable.insertLast(ar);
            }

            return true;
        }

        logger.error("ERROR: Cannot deliver event: Failed to find activity " + e.getTarget());

        return false;
    }

    protected ActivityRecord[] steal(AbstractContext context, StealStrategy s, boolean allowRestricted, int count,
            ConstellationIdentifier source) {

        steals++;

        ActivityRecord[] result = new ActivityRecord[count];

        if (logger.isTraceEnabled()) {
            logger.trace("STEAL BASE(" + identifier + "): activities F: " + fresh.size() + " W: "
                    + /* wrongContext.size() + */" R: " + runnable.size() + " L: " + lookup.size());
        }

        int r = 0;

        if (allowRestricted) {
            r = restricted.steal(context, s, result, 0, count);
        }
        if (r < count) {
            r += fresh.steal(context, s, result, r, count - r);
        }

        if (r != 0) {
            for (int i = 0; i < r; i++) {
                if (result[i].isStolen()) {
                    // Sanity check, should not happen.
                    logger.warn("INTERNAL ERROR: return stolen job " + identifier);
                }

                lookup.remove(result[i].identifier());

                if (logger.isTraceEnabled()) {
                    logger.trace("STOLEN " + result[i].identifier());
                }
            }
            stolenJobs += r;
            stealSuccess++;
        }
        
        /** TD: r Activities have been removed from our queues (r could be zero) */    
        decActivities(r);
        
        return r != 0? result : null;
    }
    
    /**
     * 
     * @param tmp
     * @return {@code true}  - the activity is done with and removed <br/>
     *         {@code false} - the activity is inserted in the {@link #runnable} queue
     * @postcondition
     * The contract is that when {@code false} is returned {@code tmp} is put back in a queue
     * and when {@code true} is returned {@code tmp} is not put back in the queue.
     */
    private boolean process(ActivityRecord tmp) {
        int evt = 0;

        TimerImpl timer = tmp.isFinishing() ? cleanupTimer : tmp.isRunnable() ? processTimer : initializeTimer;

        if (PROFILE) {
            evt = timer.start();
        }

        tmp.run(this);

        if (PROFILE) {
            timer.stop(evt);
        }

        if (tmp.needsToRun()) {
            

            runnable.insertFirst(tmp);
            
            /** TD: dequeue() could have removed from runnable but we handle that inside process()
             *      Thus, we are safe to increment here */
            
            incActivities(1);
            
            return false;
            
        } else if (tmp.isDone()) {
            
            /** TD: If we remove something with cancel we handle it inside it */
            if ( cancel(tmp.identifier()) ) {
                /** Q: I think this should never happen? */
            }
        }
        
        return true;
        
    }

    
    /**
     * Dequeue an activity an run it.
     * Only activities this executor can run are processed.
     * When {@code false} is returned, it is possible that
     * activities of different context are still in queue
     * 
     * @return {@code true} - We processed an activity
     *         {@code false} - The queue was empty and we processed nothing
     */
    public boolean process() throws TerminationDetectedException {

        ActivityRecord tmp = dequeue();

        // NOTE: the queue is guaranteed to only contain activities that we can
        // run. Whenever new activities are added or the context of
        // this constellation changes we filter out all activities that do not
        // match.

        if (tmp != null) {
            
            /** TD: we certainly dequeued something, however have to first execute the activity 
             *      So lets decrement and check for termination after the process() returns */
            
            if(!terminationDetectionStarted) //this should never happen
                System.out.println("PANIC");
            if (logger.isDebugEnabled()) {
                logger.debug("Processing activity " + tmp.identifier());
            }
            if ( process(tmp) ) {
                /* Check for termination after we have processed an activity 
                 * 
                 * We cannot check with attemptTermination() here because this
                 * method is called by the parent SingleThreadedConstellation 
                 * so if termination occurs here let it know by throwing an exception.
                 * and let parent handle it.
                 * */
                
                /** TD: This process() is called by the parent. devActivities with decrement by 1 and then
                 *      check for termination. If terminated we call parent.informTerminated(this) which in
                 *      turn and if has empty queues will in turn call parent.informTerminated(this) and if
                 *      all other workers are passive local termination is announced. My point is, the following
                 *      decActivities call could potentially make the MultiThreadedConstellation to announce. 
                 *      Is this ok? Or maybe we can throw an exception instead here? 
                 **/
                decActivities(1);
                
            } else {
                /* We do nothing because tmp is put back in queue */
            }
            
            return true;
        }
        
        return false;
    }

    public TimerImpl getInitializeTimer() {
        return initializeTimer;
    }

    public TimerImpl getProcessTimer() {
        return processTimer;
    }

    public TimerImpl getCleanupTimer() {
        return cleanupTimer;
    }

    public long getActivitiesSubmitted() {
        return activitiesSubmitted;
    }

    public long getWrongContextSubmitted() {
        return wrongContextSubmitted;
    }

    public long getMessagesInternal() {
        return messagesInternal;
    }

    public long getMessagesExternal() {
        return messagesExternal;
    }

    public TimerImpl getMessagesTimer() {
        return messagesTimer;
    }

    public long getSteals() {
        return steals;
    }

    public long getStealSuccess() {
        return stealSuccess;
    }

    public long getStolen() {
        return stolenJobs;
    }

    @Override
    public ConstellationIdentifierImpl identifier() {
        return identifier;
    }

    @Override
    public boolean isMaster() {
        return parent.isMaster();
    }

    @Override
    public boolean activate() {
        return parent.performActivate();
    }

    public boolean processActivities() {
        return parent.processActivities();
    }

    public void runExecutor() {
        if (logger.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder("\nStarting Executor: " + identifier() + "\n");

            sb.append("        context: " + getContext() + "\n");
            sb.append("           pool: " + belongsTo() + "\n");
            sb.append("    steals from: " + stealsFrom() + "\n");
            sb.append("          local: " + getLocalStealStrategy() + "\n");
            sb.append("  constellation: " + getConstellationStealStrategy() + "\n");
            sb.append("         remote: " + getRemoteStealStrategy() + "\n");
            sb.append("--------------------------");

            logger.info(sb.toString());
        }

        boolean done = false;
        
        
        try {
            while (!done) {
                done = processActivities();
            }
        } catch (TerminationDetectedException e) {
            System.out.println("Executor Termination");
        } catch (Throwable e) {
            logger.error("Executor terminated unexpectedly!", e);
        }

        logger.info("Executor done!");
        
        //Check if queues empty here?
    }

    @Override
    public TimerImpl getTimer(String standardDevice, String standardThread, String standardAction) {
        return parent.getProfiling().getTimer(standardDevice, standardThread, standardAction);
    }

    @Override
    public TimerImpl getTimer() {
        return parent.getProfiling().getTimer();
    }

    @Override
    public TimerImpl getOverallTimer() {
        return parent.getProfiling().getOverallTimer();
    }

    public int getJobLimit() {
        return QUEUED_JOB_LIMIT;
    }

    /**
     * Returns the steal pool this executor steals from.
     *
     * @return the steal pool this executor steals from
     */
    public StealPool stealsFrom() {
        return stealsFrom;
    }

    /**
     * Returns the local steal strategy, which is the strategy used when this executor is stealing from itself.
     *
     * @return the local steal strategy
     */
    public StealStrategy getLocalStealStrategy() {
        return localStealStrategy;
    }

    /**
     * Returns the steal strategy used when stealing within the current constellation (but from other executors).
     *
     * @return the steal strategy for stealing within the current constellation
     */
    public StealStrategy getConstellationStealStrategy() {
        return constellationStealStrategy;
    }

    /**
     * Returns the steal strategy used when stealing from other constellation instances.
     *
     * @return the remote steal strategy
     */
    public StealStrategy getRemoteStealStrategy() {
        return remoteStealStrategy;
    }

    /**
     * Returns the context of this executor.
     *
     * @return the executor's context
     */
    public AbstractContext getContext() {
        return myContext;
    }

    /**
     * Returns the steal pool that this executor belongs to.
     *
     * @return the steal pool this executor belongs to.
     */
    public StealPool belongsTo() {
        return myPool;
    }
    
    
    
    /** TERMINATION STUFF */
    
    /** TD: keeps track of how many activities reside in some local queue */
    private AtomicInteger activitiesInQueue;
    
    /** TD: When {@code true} there's either at least 1 activity 
     *      in some queue or we are executing one (or both)
     *      
     *      When {@code false} all queues are empty and no 
     *      activity currently executing */
    private boolean terminationDetectionStarted;
    
    
    
    private void terminationDetectionInitialize() {
        this.activitiesInQueue = new AtomicInteger(0);
        this.terminationDetectionStarted = false;
    }
    
    private void incActivities(int by) {
        if (by != 0) {
            this.activitiesInQueue.addAndGet(by);
            
            if (!this.terminationDetectionStarted)
                terminationDetectionStarted = true;
        }
    }
    
    private void decActivities(int by) {
        if (by != 0) {
            this.activitiesInQueue.addAndGet(-by);
            
            if ( terminationCheck() ) {
                performTermination();
            }
        }
    }
    
    private boolean terminationCheck() {
        return this.terminationDetectionStarted && this.activitiesInQueue.get() == 0;
    }

    @Override
    public void performTermination() {
        
        System.out.println("    Executor " + this.identifier() + " has empty queues");
        parent.informTerminated(this);
        
    }
    
    
    
    /**
     * This method is called every time we remove something from some queue
     * It first decreases the activity count and then it checks if 0 and if
     * so it lets the parent know
     */
//    private void attemptTermination(int removed) {
//        if (removed > 0 && !this.terminationDetectionStarted) {
//            System.out.println("PANIC: Activities removed but termination detection was not started " + identifier);
//        }
//        
//        this.activitiesInQueue -= removed;
//        
//        if (this.activitiesInQueue < 0) {
//            /* Sanity check. Should never happen */
//            System.out.println("PANIC: Activity count less than 0. " + identifier);
//            logger.error("EXECUTOR-TERM-DETECT-ERROR: activitiesInQueue less than 0," + identifier);
//        }
//        
//        if(this.terminationDetectionStarted && activitiesInQueue == 0)
//            performTermination();
//    }

}

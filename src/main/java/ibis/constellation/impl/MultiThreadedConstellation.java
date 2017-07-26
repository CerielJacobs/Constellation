package ibis.constellation.impl;

import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
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
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.ConstellationProperties;
import ibis.constellation.Context;
import ibis.constellation.Event;
import ibis.constellation.NoSuitableExecutorException;
import ibis.constellation.OrContext;
import ibis.constellation.StealPool;
import ibis.constellation.impl.util.Profiling;

import ibis.constellation.impl.termination.Terminateable;
import ibis.constellation.impl.termination.Terminator;

public class MultiThreadedConstellation implements Terminateable, Terminator<SingleThreadedConstellation> {

    private static final Logger logger = LoggerFactory.getLogger(MultiThreadedConstellation.class);

    private final DistributedConstellation parent;

    private final SingleThreadedConstellation[] workers;

    private final boolean[][] poolMatrix;

    private final int workerCount;

    private final ConstellationIdentifierImpl identifier;

    private final Random random = new Random();

    private boolean active = false;

    private final AbstractContext myContext;

    private final ConstellationIdentifierFactory cidFactory;

    private final int localStealSize;

    private final Profiling profiling;

    private final Facade facade = new Facade();

    private final String PROFILE_OUTPUT;

    private final boolean PROFILE;

    private class Facade implements Constellation {

        /* Following methods implement the Constellation interface */

        @Override
        public ActivityIdentifier submit(Activity activity) throws NoSuitableExecutorException {
            return performSubmit(activity);
        }

        @Override
        public void send(Event e) {
            if (!((ActivityIdentifierImpl) e.getTarget()).expectsEvents()) {
                throw new IllegalArgumentException("Target activity " + e.getTarget() + "  does not expect an event!");
            }

            // An external application wishes to send an event to 'e.target'.
            performSend(e);
        }
        //
        // @Override
        // public void cancel(ActivityIdentifierImpl aid) {
        // // ignored!
        // }

        @Override
        public boolean activate() {
            return MultiThreadedConstellation.this.activate();
        }

        @Override
        public void done() {
            if (logger.isInfoEnabled()) {
                logger.info("Calling performDone");
            }
            MultiThreadedConstellation.this.done();
        }

        @Override
        public boolean isMaster() {
            return parent == null;
        }

        @Override
        public ConstellationIdentifier identifier() {
            return identifier;
        }

        @Override
        public TimerImpl getTimer(String standardDevice, String standardThread, String standardAction) {
            return profiling.getTimer(standardDevice, standardThread, standardAction);
        }

        @Override
        public TimerImpl getTimer() {
            return profiling.getTimer();
        }

        @Override
        public TimerImpl getOverallTimer() {
            return profiling.getOverallTimer();
        }
    }

    public MultiThreadedConstellation(DistributedConstellation parent, ConstellationProperties properties,
            ConstellationConfiguration[] c) throws ConstellationCreationException {

        this.parent = parent;

        if (parent != null) {
            cidFactory = parent.getConstellationIdentifierFactory();
            identifier = parent.identifier();
        } else {
            cidFactory = new ConstellationIdentifierFactory(0);
            identifier = cidFactory.generateConstellationIdentifier();
        }

        PROFILE = properties.PROFILE;
        PROFILE_OUTPUT = properties.PROFILE_OUTPUT;

        localStealSize = properties.STEAL_SIZE;

        if (logger.isInfoEnabled()) {
            logger.info("MultiThreaded: steal size set to " + localStealSize);
            logger.info("Starting MultiThreadedConstellation " + identifier);
        }

        if (parent != null) {
            profiling = parent.getProfiling();
        } else {
            profiling = new Profiling(identifier.toString());
        }

        workers = new SingleThreadedConstellation[c.length];
        for (int i = 0; i < c.length; i++) {
            workers[i] = new SingleThreadedConstellation(this, c[i], properties);
            workers[i].setRank(i);
        }
        workerCount = workers.length;

        StealPool[] stealsFrom = new StealPool[workerCount];
        StealPool[] belongsTo = new StealPool[workerCount];

        poolMatrix = new boolean[workerCount][workerCount];

        for (int i = 0; i < workerCount; i++) {
            belongsTo[i] = workers[i].belongsTo();
            stealsFrom[i] = workers[i].stealsFrom();
        }

        for (int i = 0; i < workerCount; i++) {
            for (int j = 0; j < workerCount; j++) {
                poolMatrix[i][j] = stealsFrom[i].overlap(belongsTo[j]);
            }
        }

        myContext = mergeContext();

        if (parent != null) {
            parent.belongsTo(belongsTo);
            parent.stealsFrom(stealsFrom);
        }

    }

    public Profiling getProfiling() {
        return profiling;
    }

    private int next = 0;
    
    /**
     * TD: The first time this method is called we initiate termination detection for this Constellation
     * 
     * Q: I guess submit is the only way to start doing work?
     * 
     * @param activity
     * @return
     * @throws NoSuitableExecutorException
     */
    public synchronized ActivityIdentifier performSubmit(Activity activity) throws NoSuitableExecutorException {
        System.out.println("performSubmit() multi " + identifier);
        for (int i = 0; i < workerCount; i++) {
            // Round robin submit (for testing)
            int index = next++;
            next = next % workerCount;
            SingleThreadedConstellation e = workers[index];

            if (ContextMatch.match(e.getContext(), activity.getContext())) {
                
                /** TD: Start termination detection if not started already. This check is needed because (i think) 
                 *      it is possible to call submit multiple times from the application? */
                if ( !terminationDetectionStarted ) 
                    terminationDetectionInitiate("Termination Detection started by performSubmit(1)");
                
                /** TD: We certainly pass something down so register an active worker */
                activeWorker(e.identifier());
                
                return e.performSubmit(activity);
            }
        }
        
        if (logger.isInfoEnabled()) {
            logger.info("No local executor for this activity (no identifier yet)");
        }

        if (parent == null) {
            throw new NoSuitableExecutorException("submit: no suitable executor found");
        }

        // Try to find a worker with WORLD stealpool
        for (int i = 0; i < workerCount; i++) {
            int index = next++;
            next = next % workerCount;
            SingleThreadedConstellation e = workers[index];

            if (e.belongsTo().isWorld()) {
                
                /** TD: Start termination detection if not started already. This check is needed because (i think) 
                 *      it is possible to call submit multiple times from the application? */
                if ( !terminationDetectionStarted )
                    terminationDetectionInitiate("Termination Detection started by performSubmit(2)");
                
                /** TD: We certainly pass something down so register an active worker */
                activeWorker(e.identifier());
                
                return e.performSubmit(activity);
            }
        }

        // 
        int i = next++;
        next = next % workerCount;
        
        
        /** Q: We pass something down no matter what? */
        
        /** TD: Start termination detection if not started already. This check is needed because (i think) 
         *      it is possible to call submit multiple times from the application? */
        if ( !terminationDetectionStarted ) 
            terminationDetectionInitiate("Termination Detection started by performSubmit(3)");
        
        
        /** TD: We certainly pass something down so register an active worker */
        activeWorker(workers[i].identifier());
        
        return workers[i].performSubmit(activity);

    }

    public void performSend(Event e) {

        // Since we don't known where the target activity is located, we simply
        // send the message to it's parent constellation (which may be local).
        handleEventMessage(new EventMessage(identifier, ((ActivityIdentifierImpl) e.getTarget()).getOrigin(), e));
    }

    public void performCancel(ActivityIdentifier aid) {
        logger.error("INTERNAL ERROR: cancel not implemented!");
    }

    private SingleThreadedConstellation getWorker(ConstellationIdentifier cid) {

        for (SingleThreadedConstellation b : workers) {
            if (cid.equals(b.identifier())) {
                return b;
            }
        }

        return null;
    }

    private int selectRandomWorker() {
        // This return a random number between 0 .. workerCount-1
        return random.nextInt(workerCount);
    }

    // Delivers the specified message to the specified constellation.
    // This method returns null if either the destination constellation could
    // not be found (which is an error situation), or the message gets
    // delivered.
    // When the message cannot be delivered, the constellation identifier where
    // it should be sent instead is returned.
    private ConstellationIdentifierImpl deliverLocally(ConstellationIdentifierImpl cid, EventMessage m) {

        SingleThreadedConstellation st = getWorker(cid);

        if (st == null) {
            logger.error("TimerEvent target " + m.target + " cannot be found (event dropped)");
            return null;
        }

        return st.deliverEventMessage(m);
    }

    public void handleEventMessage(EventMessage m) {
        // One of our children wishes to send a message to 'm.target',
        // which may be local or remote.

        if (cidFactory.isLocal(m.target)) {

            ConstellationIdentifierImpl cid = deliverLocally(m.target, m);

            if (cid != null) {

                if (cid.equals(m.target)) {
                    logger.error("INTERNAL ERROR: loop in event routing! (dropping event)");
                    return;
                }

                // The activity has been relocated or stolen, so try again
                m.setTarget(cid);
                handleEventMessage(m);
            }
        } else {

            if (parent == null) {
                logger.error("TimerEvent target " + m.target + " cannot be found (event dropped)");
                return;
            }

            parent.handleApplicationMessage(m, true);
        }
    }
    
    
    /**
     * Handle a steal reply from below                          <br/><br/>
     * 
     * Called by a worker when replying to a steal request
     * 
     * @param src
     *      The owner of the reply
     * @param m
     *      The steal reply. {@code m.target} is the worker this reply 
     *      will be delivered to.
     * @return
     */ 
    public boolean handleStealReply(SingleThreadedConstellation src, StealReply m) {
        SingleThreadedConstellation b = getWorker(m.target);
        if (b != null) {
            
            if ( !m.isEmpty() ) {
                /** TD: The reply contains a number of activities */
                activeWorker(b.identifier());
            }
            
            b.deliverStealReply(m);
            
//            if(!m.isEmpty()) {
//                this.activatedWorkers.put(b.identifier(), true);
//            }
            return true;
        }

        if (parent != null) {
            /** TD: This may be relevant to distributed term. detection */
            return parent.handleStealReply(m);
        }

        logger.error("Received steal reply for unknown target " + m.target + " (reclaiming work and dropping reply)");
        return false;
    }
    
    /**
     * Handle a steal request from below.
     * 
     * @param c
     *      The source of the request
     * @param stealSize
     *      The {@link ConstellationProperties#STEAL_SIZE} of the the requesting worker
     *      
     * @return
     */
    public ActivityRecord[] handleStealRequest(final SingleThreadedConstellation c, final int stealSize) {
        // a steal request from below
        final AbstractContext context = c.getContext();
        final StealPool pool = c.stealsFrom();

        if (logger.isTraceEnabled()) {
            logger.trace("M STEAL REQUEST from child " + c.identifier() + " with context " + context + " to pool " + pool);
        }

        // First attempt to satisfy the request locally without waiting for
        // anyone
        final int rnd = selectRandomWorker();
        final int rank = c.getRank();

        final ActivityRecord[] result = new ActivityRecord[localStealSize];

        for (int i = 0; i < workerCount; i++) {

            final SingleThreadedConstellation tmp = workers[(rnd + i) % workerCount];

            if (!tmp.equals(c) && poolMatrix[rank][tmp.getRank()]) {

                final int size = tmp.attemptSteal(result, context, c.getConstellationStealStrategy(), pool, c.identifier(),
                        localStealSize, true);

                if (size > 0) {
                    
                    /** TD: We pass *size activities down ? */
                    
                    if ( !terminationDetectionStarted )
                        terminationDetectionInitiate("Termination Detection started by handleStealRequest()");
                    
                    //activeWorker(c.identifier());
                    
                    return result;
                }
            }
        }

        // If this fails, we do a remote steal followed by an enqueued steal at
        // a random suitable peer.
        final StealRequest sr = new StealRequest(c.identifier(), context, c.getLocalStealStrategy(),
                c.getConstellationStealStrategy(), c.getRemoteStealStrategy(), pool, stealSize);

        if (parent != null) {
            parent.handleStealRequest(sr);
        }

        for (int i = 0; i < workerCount; i++) {

            final SingleThreadedConstellation tmp = workers[(rnd + i) % workerCount];

            if (!tmp.equals(c) && poolMatrix[rank][tmp.getRank()]) {
                /** TD: we just queue the request so nothing is activated? */
                tmp.deliverStealRequest(sr);
                return null;
            }
        }

        return null;
    }

    public ConstellationIdentifierFactory getConstellationIdentifierFactory() {
        return cidFactory;
    }

    public synchronized AbstractContext getContext() {
        return myContext;
    }

    private AbstractContext mergeContext() {

        // We should now combine all contexts of our workers into one
        HashSet<Context> map = new HashSet<>();

        for (int i = 0; i < workerCount; i++) {

            AbstractContext tmp = workers[i].getContext();

            if (tmp instanceof Context) {
                map.add((Context) tmp);
            } else {
                OrContext o = (OrContext) tmp;

                for (Context r : o) {
                    map.add(r);
                }
            }
        }

        assert (map.size() > 0);

        if (map.size() == 1) {
            return map.iterator().next();
        } else {
            return new OrContext(map.toArray(new Context[map.size()]));
        }
    }

    public ConstellationIdentifier identifier() {
        return identifier;
    }
    
    
    public boolean activate() {
        
        synchronized (this) {
            if (active) {
                return false;
            }

            active = true;
            
            /** TD: initialize termination detection */
            terminationDetectionInitialize(null);
        }

        for (int i = 0; i < workerCount; i++) {
            if (logger.isInfoEnabled()) {
                logger.info("Activating worker " + i);
            }
            workers[i].performActivate();
        }

        return true;
    }
    
    
    
    /**
     * Q: What if this is called? Should we stop termination detection and announce? 
     */
    public void done() {

        logger.info("done");

        for (SingleThreadedConstellation u : workers) {
            u.performDone();
        }

        if (PROFILE && parent == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Printing statistics");
            }
            profiling.printProfile(PROFILE_OUTPUT);
        }
    }
    
    /**
     * Handle a steal request from above
     * 
     * TD: This will be relevant to distributed term. detection 
     * 
     * @param sr
     */
    public void deliverStealRequest(StealRequest sr) {
        // steal request delivered by our parent.
        
        if (logger.isDebugEnabled()) {
            logger.debug("M REMOTE STEAL REQUEST from child " + sr.source + " context " + sr.context + " pool " + sr.pool);
        }

        final int rnd = selectRandomWorker();
        
        // Did the parent receive the StealReply correctly ?
        //boolean somethingWentWrong = false;

        // First attempt to satisfy the request without bothering anyone
        for (int i = 0; i < workerCount; i++) {

            SingleThreadedConstellation tmp = workers[(rnd + i) % workerCount];

            StealPool p = tmp.belongsTo();

            if (sr.pool.overlap(p)) {
                // We're allowed to steal!

                if (logger.isDebugEnabled()) {
                    logger.debug("Found steal target: " + tmp.identifier() + ", pool = " + p);
                }
                
                ActivityRecord[] result = tmp.attemptSteal(sr.context, sr.remoteStrategy, sr.pool, sr.source, sr.size, false);

                if (result != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("... and got a job!");
                    }
                    // We've managed to find some work!
                    if (!parent.handleStealReply(new StealReply(identifier, sr.source, sr.pool, sr.context, result))) {
                        
                        tmp.reclaim(result);
                    }

                    return;
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("... but no jobs available!");
                    }
                }
            }
        }

        // No job was found. Let's just post a request at a random location.
        for (int i = 0; i < workerCount; i++) {

            SingleThreadedConstellation tmp = workers[(rnd + i) % workerCount];

            StealPool p = tmp.belongsTo();

            if (sr.pool.overlap(p) && tmp.getWrapper().getJobLimit() > 0) {
                tmp.deliverStealRequest(sr);
                return;
            }
        }

        // No steal request was posted either. Apparently, we are not able to
        // fulfill this request in the first place! Let's send an empty
        // reply....
        parent.handleStealReply(StealReply.EMPTY(identifier, sr));
        //parent.handleStealReply(new StealReply(identifier, sr.source, sr.pool, sr.context, (ActivityRecord) null));
    }

    /**
     * Handle a steal reply from above and pass it down to
     * the worker
     * 
     * 
     * TD: This will be relevant to distributed term. detection 
     * 
     * @param sr
     */
    public void deliverStealReply(StealReply sr) {
        // steal reply delivered by our parent

        if (logger.isDebugEnabled()) {
            logger.debug("M receive STEAL reply from " + sr.source);
        }

        SingleThreadedConstellation b = getWorker(sr.target);

        if (b == null) {
            logger.error("Reveived steal reply for unknown target " + sr.target + " (selecting random target)");
            b = workers[selectRandomWorker()];
        }

        b.deliverStealReply(sr);
    }

    public void deliverEventMessage(EventMessage am) {
        // event delivered by our parent

        SingleThreadedConstellation st = getWorker(am.target);

        if (st == null) {
            logger.error("Failed to locate event target activity " + am.target + " for remote event (dropping event)");
            return;
        }

        ConstellationIdentifierImpl cid = st.deliverEventMessage(am);

        if (cid == null) {
            // Message was delivered -- we're done!
            return;
        }

        // The activity is no longer at the expected location.
        am.setTarget(cid);

        if (cidFactory.isLocal(cid)) {
            // It has been relocated
            st = getWorker(cid);

            if (st == null) {
                logger.error("Failed to locate event target activity " + am.target + " for remote event (dropping event)");
                return;
            }

            // NOTE: this should always return null!
            cid = st.deliverEventMessage(am);

        } else {
            // it has been exported
            parent.handleApplicationMessage(am, true);
        }
    }

    public Constellation getConstellation() {
        return facade;
    }


    
    
    
    /** ΤΕΡΜΙΝΑΤΙΟΝ STUFF BELOW */

    private boolean terminationDetectionStarted;
    
    private AtomicInteger activeWorkerCount; 
    
    private ConcurrentHashMap<ConstellationIdentifierImpl, Boolean> activatedWorkers;
    
    /**
     * 
     * To be called only once to initialize termination detection stuff
     * 
     * @param msg
     */
    private void terminationDetectionInitialize(String msg) {
        
        if (this.terminationDetectionStarted) {
            /* sanity check */
            logger.warn("terminationDetectionInit() called while termination detection is active " + identifier);
            
        } else {
            
            if ( msg != null ) {
                System.out.println("\n" + msg + "\n");
            } else {
                logger.warn("Initializing (local) termination detection at " + identifier);
            }

            this.activeWorkerCount = new AtomicInteger(0);
            
            this.activatedWorkers = new ConcurrentHashMap<ConstellationIdentifierImpl, Boolean>();
            
            for ( int i = 0; i < this.workers.length; i++ )
                this.activatedWorkers.put(workers[i].identifier(), false);
        }     
    }
    
    /**
     * Called to initiate a "local" termination detection process for
     * this MultiThreadedConsellation  <br/>
     */
    private void terminationDetectionInitiate(String msg) {
        if ( this.terminationDetectionStarted ) {
            /* sanity check */
            logger.warn("Termination Detection already started!");
        } else if (this.activeWorkerCount.get() != 0) {
            /* sanity check */
            logger.warn("activeWorkerCount > 0");
        } else if (activatedWorkers.values().contains(true)) {
            /* sanity check */
            logger.warn("There are workers active!");
        } else {
            /* ok */
            this.terminationDetectionStarted = true;
            
            if ( msg != null)
                System.out.println(msg);
        }
    }
    
    /**
     * TD: Called when we pass something down.                           <br/><br/>
     * 
     * If the worker was passive, we update its entry in                      <br/>
     * {@link #activatedWorkers} and increase {@link #activeWorkerCount}.<br/><br/>
     * 
     * If the worker was already active we do nothing                    <br/><br/>
     * 
     */
    public synchronized void activeWorker(ConstellationIdentifierImpl worker) {
        
        Objects.requireNonNull(worker);
        
        if (!terminationDetectionStarted) {
            /* sanity check */
            logger.info("Attempt to register active worker before Termination Detection started");
        } else {
            if ( !this.activatedWorkers.put(worker, true) ) {
                
                System.out.println("worker " + worker + " ACTIVE");
                
                this.activeWorkerCount.incrementAndGet();
                
            } else {
                /* the worker was already active. That's ok though */
            }
        }
        

           
    }
    
    /**
     * TD: Register a worker becoming passive                           <br/><br/>
     * 
     * Called by {@link #informTerminated(SingleThreadedConstellation)} when a worker
     * let's us know is passive     <br/><br/>
     * 
     * @param worker
     */
    private void passiveWorker(ConstellationIdentifierImpl worker) {
        if ( !this.activatedWorkers.put(worker, false) )
            logger.warn("Worker passive already");
        else {
            this.activeWorkerCount.decrementAndGet();
            logger.info(this.identifier + " learnt " + worker + " passive");
        }
            
    }
    
    /**
     * TD: Check for termination
     * 
     * @return {@code true} - Termination detected. All queues below are empty
     * 
     */
    private synchronized boolean terminationCheck() {
        System.out.println("Termination Check @ " + this.identifier + ": " + terminationDetectionStarted + " " + this.activeWorkerCount.get() + " " + this.activatedWorkers.values());
        return this.terminationDetectionStarted && this.activeWorkerCount.get() == 0
                && !this.activatedWorkers.values().contains(true);
    }

    
    @Override
    public void performTermination() {
        
        if (!terminationDetectionStarted) {
            /* sanity check */
            logger.warn("Attempt to performTermination() while termination detection is not started "); 
        } else {
            
            /** 1. perform anything else required for termination of this constellation here */
            terminationDetectionStarted = false;
            
            /** 2. announce the termination */
            announceTermination();
        }
    }

    @Override
    public synchronized void informTerminated(SingleThreadedConstellation t) {
        
        Objects.requireNonNull(t);
        
        passiveWorker(t.identifier());
        
        /** TD: Every time something becomes passive we check for termination  */
        if ( terminationCheck() ) {
            
            performTermination();
            
        } else {
            
        }
                   
    }

    @Override
    public void announceTermination() {
        
        /** for now just print something */
        System.out.println("TERMINATION");
        System.exit(1);
    }
    
    
}

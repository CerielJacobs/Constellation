package ibis.constellation.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.AbstractContext;
import ibis.constellation.Activity;
import ibis.constellation.ByteBufferCache;
import ibis.constellation.ByteBuffers;
import ibis.constellation.Constellation;
import ibis.constellation.Event;
import ibis.constellation.impl.util.CircularBuffer;

public class ActivityRecord implements Serializable, ByteBuffers {

    private static final Logger logger = LoggerFactory.getLogger(ActivityRecord.class);
    private static final long serialVersionUID = 6938326535791839797L;

    private static final Path tempDir;

    private static final int INITIALIZING = 1;
    private static final int SUSPENDED = 2;
    private static final int RUNNABLE = 3;
    private static final int FINISHING = 4;
    private static final int DONE = 5;
    private static final int ERROR = Integer.MAX_VALUE;

    private Activity activity;
    private final ActivityIdentifierImpl identifier;

    private final AbstractContext context;

    private final boolean mayBeStolen;

    private final CircularBuffer<Event> queue;
    private int state = INITIALIZING;

    private boolean stolen = false;
    private boolean relocated = false;
    private boolean remote = false;

    static {
        Path temp = null;

        try {
            temp = Files.createTempDirectory("Constellation");
            new File(temp.toString()).deleteOnExit();

            if (logger.isDebugEnabled()) {
                logger.debug("Created temp directory: " + temp);
            }
        } catch (IOException e) {
            e.printStackTrace();
            temp = Paths.get(System.getProperty("java.io.tmpdir"));
        } finally {
            tempDir = temp;
        }
    }

    ActivityRecord(Activity activity, ActivityIdentifierImpl id, double memoryThreshold) {
        this.activity = activity;
        this.identifier = id;
        this.context = activity.getContext();
        this.mayBeStolen = activity.mayBeStolen();

        if (activity.expectsEvents()) {
            queue = new CircularBuffer<Event>(4);
        } else {
            queue = null;
        }

        double freeRatio = (double) Runtime.getRuntime().freeMemory() / Runtime.getRuntime().totalMemory();
        if (freeRatio < memoryThreshold) {
            persistActivity();
        }
    }

    public void enqueue(Event e) {

        if (state >= FINISHING) {
            if (activity == null) {
                retrieveActivity();
            }

            throw new IllegalStateException(
                    "Cannot deliver an event to a finished activity! " + activity + " (event from " + e.getSource() + ")");
        }

        queue.insertLast(e);
    }

    public Event dequeue() {

        if (queue.size() == 0) {
            return null;
        }

        return queue.removeFirst();
    }

    public int pendingEvents() {
        return queue.size();
    }

    public ActivityIdentifierImpl identifier() {
        return identifier;
    }

    public boolean isRunnable() {
        return (state == RUNNABLE);
    }

    public boolean isFinishing() {
        return state == FINISHING;
    }

    public boolean isStolen() {
        return stolen;
    }

    public void setStolen(boolean value) {
        stolen = value;
    }

    public boolean isRemote() {
        return remote;
    }

    public void setRemote(boolean value) {
        remote = value;
    }

    public void setRelocated(boolean value) {
        relocated = value;
    }

    public boolean isRelocated() {
        return relocated;
    }

    public boolean isRestrictedToLocal() {
        return !mayBeStolen;
    }

    public boolean isDone() {
        return (state == DONE || state == ERROR);
    }

    public boolean isFresh() {
        return (state == INITIALIZING);
    }

    public boolean needsToRun() {
        return (state == INITIALIZING || state == RUNNABLE || state == FINISHING);
    }

    public boolean setRunnable() {

        if (state == RUNNABLE || state == INITIALIZING) {
            // it's already runnable
            return false;
        }

        if (state == SUSPENDED) {
            // it's runnable now
            state = RUNNABLE;
            return true;
        }

        // It cannot be made runnable
        throw new IllegalStateException("INTERNAL ERROR: activity cannot be made runnable!");
    }

    private final void runStateMachine(Constellation c) {
        try {
            int nextState;
            if (activity == null) {
                retrieveActivity();
            }

            switch (state) {

            case INITIALIZING:
                nextState = activity.initialize(c);

                if (nextState == Activity.SUSPEND) {
                    if (pendingEvents() > 0) {
                        state = RUNNABLE;
                    } else {
                        state = SUSPENDED;
                    }
                } else if (nextState == Activity.FINISH) {
                    // TODO: handle pending event here ?? Exception or warning ?
                    state = FINISHING;
                } else {
                    throw new IllegalStateException("Activity did not suspend or finish!");
                }
                break;

            case RUNNABLE:

                Event e = dequeue();

                if (e == null) {
                    throw new IllegalStateException("INTERNAL ERROR: Runnable activity has no pending events!");
                }

                nextState = activity.process(c, e);

                if (nextState == Activity.SUSPEND) {
                    // We only suspend the job if there are no pending events.
                    if (pendingEvents() > 0) {
                        state = RUNNABLE;
                    } else {
                        state = SUSPENDED;
                    }
                } else if (nextState == Activity.FINISH) {
                    // TODO: handle pending event here ?? Exception or warning ?
                    state = FINISHING;
                } else {
                    throw new IllegalStateException("Activity did not suspend or finish!");
                }

                break;

            case FINISHING:
                activity.cleanup(c);
                state = DONE;
                break;

            case DONE:
                throw new IllegalStateException("INTERNAL ERROR: Running activity that is already done");

            case ERROR:
                throw new IllegalStateException("INTERNAL ERROR: Running activity that is in an error state!");

            default:
                throw new IllegalStateException("INTERNAL ERROR: Running activity with unknown state!");
            }

        } catch (Throwable e) {
            logger.error("Activity failed: ", e);
            state = ERROR;
        }

    }

    public void run(Constellation c) {
        runStateMachine(c);
    }

    private String getStateAsString() {

        switch (state) {

        case INITIALIZING:
            return "initializing";
        case SUSPENDED:
            return "suspended";
        case RUNNABLE:
            return "runnable";
        case FINISHING:
            return "finishing";
        case DONE:
            return "done";
        case ERROR:
            return "error";
        default:
            return "unknown";
        }
    }

    @Override
    public String toString() {
        if (activity == null) {
            retrieveActivity();
        }

        return activity + " STATE: " + getStateAsString() + " " + "event queue size = " + (queue == null ? 0 : queue.size());
    }

    public AbstractContext getContext() {
        return context;
    }

    @Override
    public void pushByteBuffers(List<ByteBuffer> list) {
        if (queue != null) {
            queue.pushByteBuffers(list);
        }

        if (activity == null) {
            retrieveActivity();
        }

        if (activity != null && activity instanceof ByteBuffers) {
            ((ByteBuffers) activity).pushByteBuffers(list);
        }
    }

    @Override
    public void popByteBuffers(List<ByteBuffer> list) {
        if (queue != null) {
            queue.popByteBuffers(list);
        }

        if (activity == null) {
            retrieveActivity();
        }

        if (activity != null && activity instanceof ByteBuffers) {
            ((ByteBuffers) activity).popByteBuffers(list);
        }
    }

    private boolean persistActivity() {

        if (!activity.mayBeStolen()) {
            // some thread may access this instance
            // so it should not be flushed to disk
            return false;
        }

        File file = new File(tempDir + File.separator + identifier.toString());

        try {
            ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            file.deleteOnExit();
            oos.writeObject(activity);

            if (activity != null && activity instanceof ByteBuffers) {
                ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
                ((ByteBuffers) activity).pushByteBuffers(list);
                if (logger.isDebugEnabled()) {
                    logger.debug("Writing " + list.size() + " bytebuffers");
                }

                oos.writeInt(list.size());
                for (ByteBuffer b : list) {
                    b.position(0);
                    b.limit(b.capacity());
                    oos.writeInt(b.capacity());
                }

                for (ByteBuffer b : list) {
                    oos.write(b.array());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Wrote bytebuffer of size " + b.capacity());
                    }
                }
            }

            oos.close();
            activity = null;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Activity " + identifier + " persisted to disk " + file.getAbsolutePath());
        }

        return true;
    }

    public boolean retrieveActivity() {

        File file = new File(tempDir + File.separator + identifier.toString());

        try {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
            activity = (Activity) ois.readObject();

            if (activity != null && activity instanceof ByteBuffers) {
                int nByteBuffers = ois.readInt();
                ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();

                if (nByteBuffers > 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Reading " + nByteBuffers + " bytebuffers");
                    }

                    for (int i = 0; i < nByteBuffers; i++) {
                        int capacity = ois.readInt();
                        ByteBuffer b = ByteBufferCache.getByteBuffer(capacity, false);
                        list.add(b);
                    }

                    for (ByteBuffer b : list) {
                        b.position(0);
                        b.limit(b.capacity());
                        byte[] buffer = new byte[b.capacity()];
                        ois.read(buffer, 0, b.capacity());
                        b.put(buffer, 0, b.capacity());
                    }
                }

                ((ByteBuffers) activity).popByteBuffers(list);
            }

            ois.close();
            file.delete();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Activity " + identifier + " retrieved from disk");
        }

        return true;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        if (activity == null) {
            retrieveActivity();
        }

        oos.defaultWriteObject();
    }

    // Severe performance penalty if enabled

    //    @Override
    //    protected void finalize() throws Throwable {
    //        File file = new File(tempDir + File.separator + identifier.toString());
    //        file.delete();
    //    }

    //    public Activity getActivity() {
    //        return activity;
    //    }
}

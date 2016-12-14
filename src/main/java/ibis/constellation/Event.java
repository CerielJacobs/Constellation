package ibis.constellation;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An <code>Event</code> can be used for communication between {@link Activity activities}. A common usage is to notify an
 * activity that certain data is available, or that some processing steps have been finished. The data of an event may implement
 * the {@link ByteBuffers} interface, to send/receive any {@link java.nio.ByteBuffer}s it contains. Constellation will then call
 * the methods of this interface when needed.
 */
public final class Event implements Serializable, ByteBuffers {

    private static final long serialVersionUID = 8672434537078611592L;

    /** The source activity of this event. */
    private final ActivityIdentifier source;

    /** The destination activity of this event. */
    private final ActivityIdentifier target;

    /** The data of this event. */
    private final Object data;

    /**
     * Constructs an event with the specified parameters: a source, a target, and its data.
     *
     * @param source
     *            the source activity of this event
     * @param target
     *            the target activity for this event
     * @param data
     *            the data of this event, may be <code>null</code>
     * @throws IllegalArgumentException
     *             when either source or target is null or otherwise an illegal activity identifier (not generated by
     *             constellation). It is also thrown if the data is not null and not serializable.
     */
    public Event(ActivityIdentifier source, ActivityIdentifier target, Object data) {
        if (source == null) {
            throw new IllegalArgumentException("null provided for source of event");
        }
        if (target == null) {
            throw new IllegalArgumentException("null provided for target of event");
        }
        if (data != null && !(data instanceof Serializable)) {
            throw new IllegalArgumentException("data of event is not serializable");
        }
        source.checkActivityIdentifier();
        target.checkActivityIdentifier();
        this.source = source;
        this.target = target;
        this.data = data;
    }

    @Override
    public String toString() {
        String s = "source: " + getSource().toString();
        s += "; target: " + getTarget().toString();
        s += "; data = ";
        if (getData() != null) {
            s += getData().toString();
        } else {
            s += "none";
        }
        return s;
    }

    @Override
    public void pushByteBuffers(List<ByteBuffer> list) {
        // FIXME: This will silently fail if getData returns null or the wrong type!!
        if (getData() != null && getData() instanceof ByteBuffers) {
            ((ByteBuffers) getData()).pushByteBuffers(list);
        }
    }

    @Override
    public void popByteBuffers(List<ByteBuffer> list) {
        // FIXME: This will silently fail if getData returns null or the wrong type!!
        if (getData() != null && getData() instanceof ByteBuffers) {
            ((ByteBuffers) getData()).popByteBuffers(list);
        }
    }

    /**
     * Returns the identifier of the source activity of this event.
     *
     * @return the source activity identifier
     */
    public ActivityIdentifier getSource() {
        return source;
    }

    /**
     * Returns the identifier of the target activity of this event.
     *
     * @return the target activity identifier
     */
    public ActivityIdentifier getTarget() {
        return target;
    }

    /**
     * Returns the data object of this event.
     *
     * @return the data object
     */
    public Object getData() {
        return data;
    }
}

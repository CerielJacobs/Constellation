package ibis.constellation.impl.termination;

/**
 * A Terminatator is responsible for announcing
 * termination of a set of Terminateables.
 * 
 * For example a MutliThreadedConstellation is a
 * Terminator AND a Terminateable.
 * A SingleThreadedConstellation is a Terminateable
 * but NOT a Terminator
 * @author gkarlos
 *
 * @param <T>
 */
public interface Terminator<T extends Terminateable> {
    
    /**
     * Used by {@code Terminateable t} to announce its
     * termination to this Terminator.
     * 
     * @param t
     */
    void informTerminated(T t);
    
    
    /**
     * Perform this action when all {@code Terminateable}s under this
     * {@code Terminator}'s authority have terminated locally.
     * 
     * <strong>NOTE:</strong> For consistency, all objects that are both 
     * a {@code Terminator} and a {@code Terminateable} should adhere
     * to the following convention: 
     * 
     * <pre>
     * void performTermination() {
     *   //local termination stuff
     *   ...
     *   this.announceTermination();
     *   ...
     * }
     * 
     * void announceTermination() {
     *   ...
     *   parent.informTerminated(this);
     *   ...
     * }
     * </pre>
     * 
     */
    void announceTermination();
}

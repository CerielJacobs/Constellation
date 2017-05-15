package ibis.constellation.impl.termination;


/**
 * A Terminateable is something that CAN terminate 
 * 
 * @author gkarlos
 *
 */
public interface Terminateable {
    
    /**
     * Perform this action upon local termination
     * 
     */
    void performTermination();

}

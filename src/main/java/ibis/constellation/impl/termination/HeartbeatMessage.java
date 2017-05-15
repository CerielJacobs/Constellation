package ibis.constellation.impl.termination;

import java.io.Serializable;

import ibis.constellation.ConstellationIdentifier;

public class HeartbeatMessage implements Serializable, Comparable<HeartbeatMessage> {

    /**
     * Special purpose message to indicate that the sender is still alive
     * 
     * 
     * This class has to extend Event?
     */
    private static final long serialVersionUID = 1L;
    
    /** Set by the sender on send */
    private final long timestamp;
    private ConstellationIdentifier sender;
    
    public HeartbeatMessage(ConstellationIdentifier sender) {
        this.timestamp = System.currentTimeMillis();
        this.sender = sender;
    }
    
    /** Copy constructor just in case */
    public HeartbeatMessage(HeartbeatMessage hm) {
        this.timestamp = hm.timestamp();
        this.sender = hm.sender();
    }
    
    public long timestamp() {
        return this.timestamp;
    }
    
    public ConstellationIdentifier sender() {
        return this.sender;
    }
    
    @Override
    public String toString() {
        return Long.toString(timestamp);
    }
    
    @Override
    public boolean equals(Object o) {
        if( this == o)
            return true;
        
        if(o == null)
            return false;
        
        if(!(o instanceof HeartbeatMessage))
            return false;
        
        HeartbeatMessage hm = (HeartbeatMessage) o;
        
        return this.timestamp() == hm.timestamp();
    }
    
    @Override
    public int hashCode() {
        int result = 3;
        int c = (int) (timestamp ^ (timestamp >>> 32));
        
        return 37 * result + c;
    }

    @Override
    public int compareTo(HeartbeatMessage o) {
        return Long.compare(timestamp(), o.timestamp());
    }
    
    
    
    
}

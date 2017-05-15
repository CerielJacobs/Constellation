package ibis.constellation.impl.termination;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import ibis.constellation.ConstellationIdentifier;

public class FailureDetector {

    /** Keep track of last timestamp received from a node */
    private ConcurrentHashMap<ConstellationIdentifier, Long> table;
    
    private long period;
    private ConstellationIdentifier[] identifiers;
    
    
    /** Algorithm specific */
    private Map<ConstellationIdentifier, Boolean> crashed;
    
    public FailureDetector(long period, ConstellationIdentifier[] identifiers) {
        this.period = period;
        this.identifiers = identifiers;
        
        this.table = new ConcurrentHashMap<ConstellationIdentifier, Long>();
        this.crashed = new ConcurrentHashMap<ConstellationIdentifier, Boolean>();
        
        /** Initialize the table. Assume everybody active at the beginning */
        for(ConstellationIdentifier ci: this.identifiers)
            table.put(ci, System.currentTimeMillis());
    }
    
    public void receiveHeartbeat(HeartbeatMessage hm) {
        this.table.put(hm.sender(), System.currentTimeMillis());
    }
    
    public HashSet<ConstellationIdentifier> getCrashed() {
        /** Lazy evaluation of the crashed set */
        
        /** Add some clock drift? */
        int drift = 0;
        
        /** What if a message is received while this loop is happening??? */
        for(Entry<ConstellationIdentifier, Long> e: table.entrySet()) {
            long now = System.currentTimeMillis();
            if( (e.getValue() + period + drift) < now )
                this.crashed.put(e.getKey(), true);
        }
        
        /** We need a copy */
        return new HashSet<ConstellationIdentifier>(crashed.keySet());
    }
    
    public void resetCrashed() {
        this.crashed.clear();
    }
}

package ibis.constellation.impl.termination;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ibis.constellation.ConstellationIdentifier;

/**
 * This class periodically broadcasts a heartbeat message
 * 
 * @author gkarlos
 */
public class HeartbeatSender {

    private ConstellationIdentifier me;
    private int period;
    private Runnable sendHeartbeats;
    private ScheduledExecutorService executor;
    
    public HeartbeatSender(ConstellationIdentifier myIdentifier, int period) {
        this.me = myIdentifier;
        this.period = period;
        
        this.sendHeartbeats =  new Runnable(){

            @Override
            public void run() {
                // TODO Auto-generated method stub
                
            }
        };
        
        executor = Executors.newScheduledThreadPool(1);
    }
    
    public void start() {
        executor.scheduleAtFixedRate(sendHeartbeats, period, period, TimeUnit.MILLISECONDS);
    }
    
    public void stop() {
        executor.shutdown();
    }
}

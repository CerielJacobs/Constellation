package ibis.constellation.impl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.ByteBufferCache;
import ibis.constellation.ByteBuffers;
import ibis.constellation.impl.ActivityRecord;

public class ActivityRecordCache {
	
	private static final Logger logger = LoggerFactory.getLogger(ActivityRecordCache.class);
	
	// If the free memory to total memory ratio is found to be below this threshold, then
	// the newly enqueued activity will be persisted to disk.
	private static final double FREE_MEMORY_RATIO_THRESHOLD = 0.2;
	
	private String cachePath;
	private File cache;
	private int size;
	private Map<ActivityIdentifier, ActivityRecord> map;
	
	public ActivityRecordCache(String cacheName) {
		cachePath = System.getProperty("java.io.tmpdir") + File.separator + "Constellation" + File.separator + cacheName;
		cache = new File(cachePath);
		cache.mkdirs();
		cache.deleteOnExit();
		map = new HashMap<ActivityIdentifier, ActivityRecord>();
		
		if (logger.isTraceEnabled()) {
			logger.trace("Created ActivityRecordCache: " + cacheName);
		}
	}
	
	public void put(ActivityIdentifier id, ActivityRecord ar) {
		
		double freeRatio = (double) Runtime.getRuntime().freeMemory() / Runtime.getRuntime().totalMemory();
		if (freeRatio < FREE_MEMORY_RATIO_THRESHOLD) {
			if (persistActivityRecord(ar)) {
				ar = null;
				
				if (logger.isDebugEnabled()) {
					logger.debug("Persisted Activity to disk: " + id);
				}
			}
		}
		
		map.put(id, ar);
	}
	
	private boolean persistActivityRecord(ActivityRecord ar) {
		if (ar.isRestrictedToLocal()) {
			// some thread may access this instance
			// so it should not be persisted
			return false;
		}
		
		try {
			File file = new File(cachePath + File.separator + ar.identifier());
			file.delete();
			file.createNewFile();
			file.deleteOnExit();
			
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(ar);
			
			if (ar != null && ar instanceof ByteBuffers) {
				ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
				((ByteBuffers) ar).pushByteBuffers(list);
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
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public ActivityRecord get(ActivityIdentifier id) {
		ActivityRecord ar = null;
		
		if (map.containsKey(id)) {
			ar = map.get(id);
			
			if (ar == null) {
				ar = retrieveActivityRecord(id);
			}
			
			map.put(id, ar);
		}
		
		return ar;
	}
	
	private ActivityRecord retrieveActivityRecord(ActivityIdentifier id) {
		ActivityRecord ar = null;
		
		try {
			File file = new File(cachePath + File.separator + id);
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			ar = (ActivityRecord) ois.readObject();
			
			if (ar != null && ar instanceof ByteBuffers) {
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
                
                ((ByteBuffers) ar).popByteBuffers(list);
            }
			
			ois.close();
			file.delete();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		
		if (logger.isDebugEnabled()) {
			logger.debug("Retrieved Activity from disk: " + ar.identifier());
		}
		
		return ar;
	}
	
	public ActivityRecord remove(ActivityIdentifier id) {
		ActivityRecord ar = get(id);
		
		if (ar != null) {
			map.remove(id);
		}
		
		return ar;
	}
	
	public int size() {
		return size;
	}
}

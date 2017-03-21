package ibis.constellation.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.Activity;
import ibis.constellation.ByteBufferCache;
import ibis.constellation.ByteBuffers;

public class ActivityWrapper {
	
	private static final Logger logger = LoggerFactory.getLogger(ActivityWrapper.class);
	
	private Activity activity;
	private final ActivityIdentifierImpl identifier;
	private final File file;
	
	public ActivityWrapper(Activity activity, ActivityIdentifierImpl id) {
		this.activity = activity;
        this.identifier = id;
        this.file = new File(System.getProperty("java.io.tmpdir") + "/" + identifier.toString());
	}
	
	public Activity getActivity() {
		
		if (activity == null) {
			if (!retrieveActivity()) {
				if (logger.isErrorEnabled()) {
                    logger.error("Failed to retrieve activity " + identifier + " from file");
                }
			}
		}
		
		return activity;
	}
	
	public boolean persistActivity() {
		
		if (!activity.mayBeStolen()) {
			// some thread may access this instance
			// so it should not be flushed to disk
			return false;
		}
		
		try {
			file.delete();
			file.createNewFile();
			file.deleteOnExit();
			
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
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
		
		return true;
	}
	
	private boolean retrieveActivity() {
		
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
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
		
		return true;
	}
}

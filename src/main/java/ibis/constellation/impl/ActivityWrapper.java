package ibis.constellation.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.Activity;
import ibis.constellation.ByteBuffers;

public class ActivityWrapper {
	
	private static final Logger logger = LoggerFactory.getLogger(ActivityWrapper.class);
	
	private Activity activity;
	private final ActivityIdentifierImpl identifier;
	
	public ActivityWrapper(Activity activity, ActivityIdentifierImpl id) {
		this.activity = activity;
        this.identifier = id;
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
		
		File file = new File(System.getProperty("java.io.tmpdir") + "/" + identifier.toString());
		
		try {
			file.delete();
			file.createNewFile();
			file.deleteOnExit();
			
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(activity);
			
			if (activity != null && activity instanceof ByteBuffers) {
				
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
		
		File file = new File(System.getProperty("java.io.tmpdir") + "/" + identifier.toString());
		
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
			activity = (Activity) ois.readObject();
			ois.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
}

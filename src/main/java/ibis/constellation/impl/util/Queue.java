package ibis.constellation.impl.util;

import ibis.constellation.impl.ActivityRecord;

public interface Queue {
	void enqueue(ActivityRecord a) throws Exception;
	ActivityRecord dequeue() throws Exception;
	boolean removeByReference(ActivityRecord o);
	int size();
}

package ibis.constellation.impl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.impl.ActivityRecord;

public class BucketSortedRangeList implements SortedRangeList {

    public static final Logger log = LoggerFactory.getLogger(BucketSortedRangeList.class);
    
    static class Bucket {
    	private Bucket next;
    	private Bucket prev;
    	
    	private final long start;
    	private final long end;
    	
    	private final Queue queue;
    	
    	Bucket(long start, long end) {
    		this.start = start;
    		this.end = end;
    		this.queue = new SimpleQueue();
    	}
    }

    private final String name;

    private Bucket head = new Bucket(Long.MIN_VALUE, Long.MIN_VALUE);
    private Bucket tail = new Bucket(Long.MAX_VALUE, Long.MAX_VALUE);
    private int size;

    public BucketSortedRangeList(String name) {
        this.name = name;
        head.next = tail;
        tail.prev = head;
        size = 0;
    }

    public void insert(ActivityRecord a, long start, long end) {
    	
    	// Check if a bucket for the specified start and end values exists.
    	// If not, create one at the appropriate position.
        Bucket current = head.next;

        for (;;) {
            if (start == current.start && end == current.end)
            	break;
            
            if (start < current.start || (start == current.start && end < current.end)) {
            	
            	Bucket b = new Bucket(start, end);

                b.prev = current.prev;
                current.prev.next = b;

                b.next = current;
                current.prev = b;
                
                current = b;
                break;
            }

            current = current.next;
        }
        
        try {
			current.queue.enqueue(a);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
        size++;
    }
    
    // Returns the first ActivityRecord in b's queue, and deletes b if its queue
    // becomes empty. Also adjusts the size value.
    private ActivityRecord dequeueFromBucket(Bucket b) {
    	ActivityRecord a = null;
		try {
			a = b.queue.dequeue();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
        size--;
        
        if (b.queue.size() == 0) {
        	b.prev.next = b.next;
        	b.next.prev = b.prev;
        }
        
        return a;
    }

    public ActivityRecord removeHead() {

        if (size == 0) {
            return null;
        }
        
        // Return the first record of the first bucket
        Bucket b = head.next;
        ActivityRecord a = dequeueFromBucket(b);

        return a;
    }

    public ActivityRecord removeTail() {

        if (size == 0) {
            return null;
        }

        // Return the first record of the last bucket
        Bucket b = tail.prev;
        ActivityRecord a = dequeueFromBucket(b);

        return a;
    }

    public int size() {
        return size;
    }

    public boolean removeByReference(ActivityRecord o) {
    	
    	Bucket current = head.next;
    	
    	while (current != tail) {
    		if (current.queue.removeByReference(o)) {
    			break;
    		}
    		
    		current = current.next;
    	}

        return false;
    }

    public ActivityRecord removeSmallestInRange(long start, long end) {

        if (size == 0) {
            return null;
        }
        
        Bucket current = head.next;

        while (current != tail && current.end < start) {
            current = current.next;
        }

        if (current == tail || end < current.start) {
            return null;
        }

        // Found a bucket with overlap!
        ActivityRecord a = dequeueFromBucket(current);

        return a;
    }

    public ActivityRecord removeBiggestInRange(long start, long end) {

        if (size == 0) {
            return null;
        }
        
        Bucket current = tail.prev;

        while (current != head && end < current.start) {
            current = current.prev;
        }

        if (current == head || current.end < start) {
            return null;
        }

        // Found it
        ActivityRecord a = dequeueFromBucket(current);

        return a;
    }

    
    public String getName() {
        return name;
    }
}

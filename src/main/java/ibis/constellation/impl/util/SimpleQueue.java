package ibis.constellation.impl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.impl.ActivityRecord;

public class SimpleQueue implements Queue {
	
	private static final Logger logger = LoggerFactory.getLogger(SimpleQueue.class);
	
	// If the free memory to max memory ratio is found to be below this threshold, then
	// the newly enqueued activity will be persisted to disk.
	private static final double FREE_MEMORY_RATIO_THRESHOLD = 0.2;
	
	static class Node {
		private Node next;
		private Node prev;
		
		private ActivityRecord data;
		
		Node(ActivityRecord data) {
			this.data = data;
		}
	}
	
	private Node head = new Node(null);
	private Node tail = new Node(null);
	
	private int size;
	
	public SimpleQueue() {
		head.next = tail;
		tail.prev = head;
		size = 0;
	}

	@Override
	public void enqueue(ActivityRecord a) {
		
		Node n = new Node(a);
		
		n.prev = tail.prev;
		n.prev.next = n;
		
		n.next = tail;
		tail.prev = n;
		
		size++;
		
		double freeRatio = (double) Runtime.getRuntime().freeMemory() / Runtime.getRuntime().maxMemory();
		if (freeRatio < FREE_MEMORY_RATIO_THRESHOLD) {
			a.persistActivity();
			
			if (logger.isDebugEnabled()) {
				logger.debug("Activity " + a.identifier() + " persisted to disk");
			}			
		}
	}

	@Override
	public ActivityRecord dequeue() {
		
		if (size == 0) {
			return null;
		}
		
		Node n = head.next;
		
		head.next = n.next;
		n.next.prev = head;
		size--;
		
		return n.data;
	}
	
	@Override
	public boolean removeByReference(ActivityRecord o) {
		
		Node current = head.next;

        while (current.data != null) {

            if (current.data == o) {
                // Found it
                current.prev.next = current.next;
                current.next.prev = current.prev;
                size--;
                return true;
            }

            current = current.next;
        }

        return false;
	}

	@Override
	public int size() {
		return size;
	}

}

package ibis.constellation.impl.util;

import ibis.constellation.impl.ActivityRecord;

public interface SortedRangeList {

    void insert(ActivityRecord a, long start, long end);

    ActivityRecord removeHead();

    ActivityRecord removeTail();

    int size();

    boolean removeByReference(ActivityRecord o);

    ActivityRecord removeSmallestInRange(long start, long end);

    ActivityRecord removeBiggestInRange(long start, long end);

    public String getName();
}

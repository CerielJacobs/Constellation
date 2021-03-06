package test.pipeline.inbalance;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.Context;
import ibis.constellation.NoSuitableExecutorException;
import ibis.constellation.util.SimpleActivity;

public class Stage1 extends SimpleActivity {

    private static final long serialVersionUID = -3987089095770723454L;

    private final long sleep;
    private final Data data;

    public Stage1(ActivityIdentifier parent, long sleep, Data data) {

        super(parent, new Context("A", data.index));

        this.sleep = sleep;
        this.data = data;
    }

    @Override
    public void simpleActivity(Constellation c) {

        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (Exception e) {
                // ignored
            }
        }

        try {
            c.submit(new Stage2(getParent(), 200, new Data(data.index, 1, data.data)));
        } catch (NoSuitableExecutorException e) {
            System.err.println("Should not happen: " + e);
            e.printStackTrace(System.err);
        }
    }
}

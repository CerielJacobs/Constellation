package test.pipeline.inbalance;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.Event;
import ibis.constellation.util.SimpleActivity;
import ibis.constellation.Context;

public class Stage3 extends SimpleActivity {

    private static final long serialVersionUID = 8278819333486874900L;

    private final long sleep;
    private final Data data;

    public Stage3(ActivityIdentifier target, long sleep, Data data) {

        super(target, new Context("X", data.index));

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

        c.send(new Event(identifier(), getParent(), new Data(data.index, 3, data.data)));
    }
}

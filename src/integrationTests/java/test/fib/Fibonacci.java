package test.fib;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationConfiguration;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Context;
import ibis.constellation.Event;
import ibis.constellation.StealStrategy;
import ibis.constellation.util.SingleEventCollector;

public class Fibonacci extends Activity {

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final boolean top;
    private final int input;
    private int output;
    private int merged = 0;

    public Fibonacci(ActivityIdentifier parent, int input, boolean top) {
        super(new Context("fib", input), true, input > 1);
        this.parent = parent;
        this.input = input;
        this.top = top;

        if (top) {
            //System.out.println("fib " + input + " created.");
        }
    }

    public Fibonacci(ActivityIdentifier parent, int input) {
        this(parent, input, false);
    }

    @Override
    public int initialize(Constellation c) {

        if (top) {
            //System.out.println("fib " + input + " running.");
        }

        if (input == 0 || input == 1) {
            output = input;
            return FINISH;
        } else {
            try {
                c.submit(new Fibonacci(identifier(), input - 1));
                c.submit(new Fibonacci(identifier(), input - 2));
            } catch (Throwable e) {
                System.err.println("Should not happen: " + e);
                e.printStackTrace(System.err);
            }
            return SUSPEND;
        }
    }

    @Override
    public int process(Constellation c, Event e) {

        if (top) {
            //System.out.println("fib " + input + " got event.");
        }

        output += (Integer) e.getData();
        merged++;

        if (merged < 2) {
            return SUSPEND;
        } else {
            return FINISH;
        }
    }

    @Override
    public void cleanup(Constellation c) {

        if (top) {
            System.out.println("fib " + input + " done.");
        }

        if (parent != null) {
            c.send(new Event(identifier(), parent, output));
        }
    }

    @Override
    public String toString() {
        return "Fib(" + identifier() + ") " + input + ", " + merged + " -> " + output;
    }

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int index = 0;

        int executors = Integer.parseInt(args[index++]);

        ConstellationConfiguration e = new ConstellationConfiguration(new Context("fib"), StealStrategy.SMALLEST,
                StealStrategy.BIGGEST, StealStrategy.BIGGEST);

        Constellation c = ConstellationFactory.createConstellation(e, executors);
        c.activate();

        int input = Integer.parseInt(args[index++]);

        if (c.isMaster()) {

            //System.out.println(c.identifier() + " Starting as master!");

            SingleEventCollector a = new SingleEventCollector(new Context("fib"));

            c.submit(a);
            c.submit(new Fibonacci(a.identifier(), input, true));

            int result = (Integer) a.waitForEvent().getData();

            //c.done();

            long end = System.currentTimeMillis();

            System.out.println("FIB: Fib(" + input + ") = " + result + " (" + (end - start) + "ms)");
        } else {
            System.out.println("Starting as slave!");
            //c.done();
        }
    }

}

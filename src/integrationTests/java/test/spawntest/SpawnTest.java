package test.spawntest;

import java.util.Properties;

import org.junit.Test;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationConfiguration;
import ibis.constellation.ConstellationCreationException;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.util.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitExecutorContext;

public class SpawnTest {

	private static final int SPAWNS_PER_SYNC = 10;
	private static final int COUNT = 1000;
	private static final int REPEAT = 5;
	private static final int CONCURRENT = 100;

	@Test
	public void test() throws ConstellationCreationException {

		Properties p = new Properties();
		p.put("ibis.constellation.distributed", "false");

		ConstellationConfiguration config = new ConstellationConfiguration(new UnitExecutorContext("TEST"),
				StealStrategy.SMALLEST, StealStrategy.BIGGEST);

		Constellation c = ConstellationFactory.createConstellation(p, config);

		c.activate();

		if (c.isMaster()) {
			for (int i = 0; i < REPEAT; i++) {
				SingleEventCollector a = new SingleEventCollector(new UnitActivityContext("TEST"));
				ActivityIdentifier id = c.submit(a);
				c.submit(new TestLoop(id, COUNT, CONCURRENT, SPAWNS_PER_SYNC));
				a.waitForEvent();
			}
		}

		c.done();
	}
}
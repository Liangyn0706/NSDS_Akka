package com.lab.evaluation26;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.lab.evaluation26.CounterCoordinatorActor;
import com.lab.evaluation26.RecordMsg;
import com.lab.evaluation26.ReportMsg;
import com.lab.evaluation26.ReportResultMsg;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/** Public-protocol acceptance tests. Every row uses a fresh coordinator/workers. */
@RunWith(Parameterized.class)
public class CounterScenariosTest {
    private static ActorSystem system;
    private final String scenario;
    private final Object[] steps;

    public CounterScenariosTest(String scenario, Object[] steps) {
        this.scenario = scenario;
        this.steps = steps;
    }

    private static RecordMsg r(int key, int value) { return new RecordMsg(key, value); }
    private static Object[] q(boolean reset, long count, long sum) {
        return new Object[] {new ReportMsg(reset), count, sum};
    }
    private static Object[] row(String name, Object... steps) {
        return new Object[] {name, steps};
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> scenarios() {
        return Arrays.asList(
            row("A01 empty false", q(false, 0, 0), q(false, 0, 0)),
            row("A02 empty true", q(true, 0, 0), q(false, 0, 0)),
            row("A03 key zero", r(0, 5), q(false, 1, 5)),
            row("A04 odd key", r(1, 7), q(false, 1, 7)),
            row("A05 both workers", r(0, 10), r(1, 7), r(2, 5), q(false, 3, 22)),
            row("A06 repeated even key", r(0, 2), r(0, 3), r(0, 4), q(false, 3, 9)),
            row("A07 identical odd records", r(3, 4), r(3, 4), q(false, 2, 8)),
            row("A08 zero key and value", r(0, 0), q(false, 1, 0)),
            row("A09 zero values both workers", r(0, 0), r(1, 0), r(0, 0), q(false, 3, 0)),
            row("A10 mixed values", r(0, 0), r(1, 7), r(0, 3), q(false, 3, 10)),
            row("A11 false preserves totals", r(0, 5), r(1, 2), q(false, 2, 7), q(false, 2, 7)),
            row("A12 accumulate after false", r(0, 5), q(false, 1, 5), r(1, 2), q(false, 2, 7)),
            row("A13 true clears both", r(0, 10), r(1, 7), q(true, 2, 17), q(false, 0, 0)),
            row("A14 reset key zero", r(0, 3), r(0, 0), q(true, 2, 3), q(false, 0, 0)),
            row("A15 reset odd only", r(1, 6), q(true, 1, 6), q(false, 0, 0)),
            row("A16 consecutive resets", r(0, 5), q(true, 1, 5), q(true, 0, 0), q(false, 0, 0)),
            row("A17 repeated key after reset", r(0, 5), q(true, 1, 5), r(0, 2), r(0, 0), q(false, 2, 2)),
            row("A18 alternate flags", r(0, 2), r(1, 3), q(false, 2, 5), q(true, 2, 5),
                q(false, 0, 0), r(1, 4), q(true, 1, 4), q(false, 0, 0)),
            row("A19 long sum robustness", r(0, Integer.MAX_VALUE), r(1, Integer.MAX_VALUE),
                q(false, 2, 4294967294L)),
            row("B07 preceding records included before reset", r(0, 5), r(1, 7),
                q(true, 2, 12), q(false, 0, 0))
        );
    }

    @BeforeClass public static void startSystem() {
        system = ActorSystem.create("CounterScenariosTest");
    }
    @AfterClass public static void stopSystem() {
        TestKit.shutdownActorSystem(system);
    }

    @Test public void followsScenario() {
        TestKit probe = new TestKit(system);
        ActorRef coordinator = system.actorOf(CounterCoordinatorActor.props());
        Duration timeout = Duration.ofMillis(Long.getLong("counter.test.timeout.ms", 2000L));
        try {
            for (Object step : steps) {
                if (step instanceof RecordMsg) {
                    coordinator.tell(step, probe.getRef());
                } else {
                    Object[] report = (Object[]) step;
                    // One sender for records and reports preserves coordinator input order.
                    // This probe tests the server protocol; production clients must use ask.
                    coordinator.tell(report[0], probe.getRef());
                    Object reply = probe.receiveOne(timeout);
                    assertNotNull(scenario + ": no report reply within " + timeout, reply);
                    assertEquals(scenario + " count", ((Number) report[1]).longValue(), number(reply, "count"));
                    assertEquals(scenario + " sum", ((Number) report[2]).longValue(), number(reply, "sum"));
                }
            }
            probe.expectNoMessage(Duration.ofMillis(100));
        } finally {
            system.stop(coordinator);
            system.stop(probe.getRef());
        }
    }

    private static long number(Object reply, String property) {
        assertTrue("Expected ReportResultMsg, received " + reply, reply instanceof ReportResultMsg);
        ReportResultMsg result = (ReportResultMsg) reply;
        return property.equals("count") ? result.getCount() : result.getSum();
    }
}

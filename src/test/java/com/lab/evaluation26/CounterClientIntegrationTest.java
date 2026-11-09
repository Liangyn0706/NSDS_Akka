package com.lab.evaluation26;

import akka.actor.*;
import akka.pattern.Patterns;
import akka.testkit.javadsl.TestKit;
import org.junit.*;
import java.time.Duration;
import static org.junit.Assert.*;

/** Runs the supplied scripts through an observing proxy without changing their content. */
public class CounterClientIntegrationTest {
    private static ActorSystem system;
    @BeforeClass public static void start() { system = ActorSystem.create("ClientIntegrationTest"); }
    @AfterClass public static void stop() { TestKit.shutdownActorSystem(system); }
    public static class Proxy extends AbstractActor {
        private final ActorRef target, observer;
        public Proxy(ActorRef target, ActorRef observer) { this.target = target; this.observer = observer; }
        @Override public Receive createReceive() {
            return receiveBuilder().match(RecordMsg.class, m -> {
                observer.tell(m, self());
                // Keep a single proxy sender for ordered target inputs.
                target.tell(m, self());
            }).match(ReportMsg.class, m -> {
                // A child relay preserves the client's temporary ask requester.
                ActorRef requester = getSender();
                ActorRef relay = getContext().actorOf(Props.create(Relay.class,
                    () -> new Relay(requester, observer)));
                // Route the result through the relay using an explicit envelope in this proxy.
                pending.add(relay);
                target.tell(m, self());
                observer.tell(m, self());
            }).match(ReportResultMsg.class, m -> {
                observer.tell(m, self()); pending.remove().tell(m, self());
            }).build();
        }
        private final java.util.Queue<ActorRef> pending = new java.util.ArrayDeque<>();
    }
    public static class Relay extends AbstractActor {
        private final ActorRef requester, observer;
        public Relay(ActorRef requester, ActorRef observer) { this.requester = requester; this.observer = observer; }
        @Override public Receive createReceive() {
            return receiveBuilder().match(ReportResultMsg.class, m -> {
                requester.tell(m, self()); getContext().stop(self());
            }).build();
        }
    }
    @Test public void originalScriptsCompleteAndConserveRecordsAcrossReset() throws Exception {
        TestKit probe = new TestKit(system);
        ActorRef coordinator = system.actorOf(CounterCoordinatorActor.props());
        ActorRef proxy = system.actorOf(Props.create(Proxy.class, () -> new Proxy(coordinator, probe.getRef())));
        ActorRef c0 = system.actorOf(CounterClientActor.props(proxy, 0));
        ActorRef c1 = system.actorOf(CounterClientActor.props(proxy, 1));
        try {
            c0.tell(new StartMsg(), probe.getRef()); c1.tell(new StartMsg(), probe.getRef());
            int records = 0, requests = 0, responses = 0;
            long resetCount = -1, resetSum = -1;
            java.util.Queue<Boolean> resets = new java.util.ArrayDeque<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (records < 10 || requests < 7 || responses < 7) {
                long remaining = deadline - System.nanoTime();
                assertTrue("Client scripts did not finish before deadline", remaining > 0);
                Object m = probe.receiveOne(Duration.ofNanos(remaining));
                assertNotNull("Missing script input or report result", m);
                if (m instanceof RecordMsg) records++;
                else if (m instanceof ReportMsg) {
                    requests++; resets.add(((ReportMsg) m).isReset());
                } else if (m instanceof ReportResultMsg) {
                    responses++;
                    if (resets.remove()) {
                        resetCount = ((ReportResultMsg) m).getCount();
                        resetSum = ((ReportResultMsg) m).getSum();
                    }
                } else fail("Unexpected event: " + m);
            }
            ReportResultMsg last = (ReportResultMsg) Patterns.ask(coordinator, new ReportMsg(false),
                Duration.ofSeconds(3)).toCompletableFuture().get(4, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(10, resetCount + last.getCount());
            assertEquals(40, resetSum + last.getSum());
        } finally {
            system.stop(c0); system.stop(c1); system.stop(proxy); system.stop(coordinator);
        }
    }
}

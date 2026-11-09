package com.lab.evaluation26;

import akka.actor.*;
import akka.testkit.javadsl.TestKit;
import org.junit.*;
import java.time.Duration;
import static org.junit.Assert.*;

public class CounterConcurrencyTest {
    private static ActorSystem system;
    private static final Duration WAIT = Duration.ofSeconds(3);
    private static final Duration QUIET = Duration.ofMillis(100);
    @BeforeClass public static void start() { system = ActorSystem.create("ConcurrencyTest"); }
    @AfterClass public static void stop() { TestKit.shutdownActorSystem(system); }

    public static final class Seen {
        final Object message; final ActorRef worker; final ActorRef sender;
        Seen(Object message, ActorRef worker, ActorRef sender) {
            this.message = message; this.worker = worker; this.sender = sender;
        }
    }
    public static final class Release { }
    public static class ControlledWorker extends AbstractActor {
        private final ActorRef observer;
        private final int id;
        private long count, sum;
        private ActorRef requester;
        private WorkerResultMsg snapshot;
        public ControlledWorker(int id, ActorRef observer) { this.id = id; this.observer = observer; }
        @Override public Receive createReceive() {
            return receiveBuilder().match(RecordMsg.class, m -> {
                count++; sum += m.getValue();
                observer.tell(new Seen(m, self(), getSender()), self());
            }).match(WorkerQueryMsg.class, m -> {
                requester = getSender();
                snapshot = new WorkerResultMsg(id, count, sum);
                if (m.isReset()) { count = 0; sum = 0; }
                observer.tell(new Seen(m, self(), getSender()), self());
            }).match(Release.class, m -> requester.tell(snapshot, self())).build();
        }
    }
    private static final class Harness {
        final TestKit even = new TestKit(system), odd = new TestKit(system), client = new TestKit(system);
        final ActorRef coordinator = system.actorOf(CounterCoordinatorActor.props(
            Props.create(ControlledWorker.class, () -> new ControlledWorker(0, even.getRef())),
            Props.create(ControlledWorker.class, () -> new ControlledWorker(1, odd.getRef()))));
        void record(int key, int value) {
            coordinator.tell(new RecordMsg(key, value), client.getRef());
            Seen seen = (key % 2 == 0 ? even : odd).expectMsgClass(WAIT, Seen.class);
            assertTrue(seen.message instanceof RecordMsg);
            assertEquals(coordinator, seen.sender);
        }
        Seen query(TestKit observer, boolean reset) {
            Seen seen = observer.expectMsgClass(WAIT, Seen.class);
            assertTrue(seen.message instanceof WorkerQueryMsg);
            assertEquals(reset, ((WorkerQueryMsg) seen.message).isReset());
            assertEquals(coordinator, seen.sender);
            return seen;
        }
        void release(Seen s) { s.worker.tell(new Release(), client.getRef()); }
        void finish() { system.stop(coordinator); }
    }
    private static void result(TestKit probe, long count, long sum) {
        ReportResultMsg r = probe.expectMsgClass(WAIT, ReportResultMsg.class);
        assertEquals(count, r.getCount()); assertEquals(sum, r.getSum());
    }

    @Test public void buffersRecordsAndReportsAcrossResetWithEitherReplyOrder() {
        for (boolean reverse : new boolean[] {false, true}) {
            Harness h = new Harness(); TestKit next = new TestKit(system);
            try {
                h.record(0, 10); h.record(2, 5); h.record(1, 7);
                h.coordinator.tell(new ReportMsg(true), h.client.getRef());
                Seen a = h.query(h.even, true), b = h.query(h.odd, true);
                h.coordinator.tell(new RecordMsg(0, 3), next.getRef());
                h.coordinator.tell(new ReportMsg(false), next.getRef());
                h.even.expectNoMessage(QUIET); h.odd.expectNoMessage(QUIET);
                h.release(reverse ? b : a);
                h.client.expectNoMessage(QUIET);
                h.even.expectNoMessage(QUIET); h.odd.expectNoMessage(QUIET);
                h.release(reverse ? a : b); result(h.client, 3, 22);
                Seen record = h.even.expectMsgClass(WAIT, Seen.class);
                assertTrue(record.message instanceof RecordMsg);
                a = h.query(h.even, false); b = h.query(h.odd, false);
                h.release(a); h.release(b); result(next, 1, 3);
                h.client.expectNoMessage(QUIET); next.expectNoMessage(QUIET);
            } finally { h.finish(); }
        }
    }

    @Test public void nonResetBuffersRecordsAndSerializesResetReports() {
        Harness h = new Harness(); TestKit other = new TestKit(system);
        try {
            h.record(0, 5);
            h.coordinator.tell(new ReportMsg(false), h.client.getRef());
            Seen a = h.query(h.even, false), b = h.query(h.odd, false);
            h.coordinator.tell(new RecordMsg(1, 7), other.getRef());
            h.coordinator.tell(new ReportMsg(true), other.getRef());
            h.even.expectNoMessage(QUIET); h.odd.expectNoMessage(QUIET);
            h.release(a); h.release(b); result(h.client, 1, 5);
            assertTrue(h.odd.expectMsgClass(WAIT, Seen.class).message instanceof RecordMsg);
            a = h.query(h.even, true); b = h.query(h.odd, true);
            h.release(b); h.release(a); result(other, 2, 12);
            h.coordinator.tell(new ReportMsg(false), h.client.getRef());
            a = h.query(h.even, false); b = h.query(h.odd, false);
            h.release(a); h.release(b); result(h.client, 0, 0);
        } finally { h.finish(); }
    }

    @Test public void queuedReportsKeepThreeDistinctRequesters() {
        Harness h = new Harness(); TestKit second = new TestKit(system), third = new TestKit(system);
        try {
            h.record(1, 7);
            h.coordinator.tell(new ReportMsg(true), h.client.getRef());
            Seen a = h.query(h.even, true), b = h.query(h.odd, true);
            h.coordinator.tell(new ReportMsg(false), second.getRef());
            h.coordinator.tell(new ReportMsg(true), third.getRef());
            h.release(a); h.release(b); result(h.client, 1, 7);
            a = h.query(h.even, false); b = h.query(h.odd, false);
            h.release(a); h.release(b); result(second, 0, 0);
            a = h.query(h.even, true); b = h.query(h.odd, true);
            h.release(a); h.release(b); result(third, 0, 0);
            h.client.expectNoMessage(QUIET); second.expectNoMessage(QUIET); third.expectNoMessage(QUIET);
        } finally { h.finish(); }
    }

    @Test public void resetBuffersRepeatedZeroValuedRecords() {
        Harness h = new Harness();
        try {
            h.record(0, 5);
            h.coordinator.tell(new ReportMsg(true), h.client.getRef());
            Seen a = h.query(h.even, true), b = h.query(h.odd, true);
            h.coordinator.tell(new RecordMsg(0, 0), h.client.getRef());
            h.coordinator.tell(new RecordMsg(0, 2), h.client.getRef());
            h.coordinator.tell(new ReportMsg(false), h.client.getRef());
            h.even.expectNoMessage(QUIET);
            h.release(a); h.release(b); result(h.client, 1, 5);
            for (int i = 0; i < 2; i++) {
                assertTrue(h.even.expectMsgClass(WAIT, Seen.class).message instanceof RecordMsg);
            }
            a = h.query(h.even, false); b = h.query(h.odd, false);
            h.release(a); h.release(b); result(h.client, 2, 2);
        } finally { h.finish(); }
    }

    @Test public void allParityRoutesUseCoordinatorSender() {
        Harness h = new Harness();
        try {
            for (int key = 0; key < 6; key++) h.record(key, key);
            h.even.expectNoMessage(QUIET); h.odd.expectNoMessage(QUIET);
        } finally { h.finish(); }
    }

    @Test public void realWorkerCapturesSnapshotsBeforeLaterRecords() {
        for (boolean reset : new boolean[] {false, true}) {
            TestKit probe = new TestKit(system);
            ActorRef worker = system.actorOf(CounterWorkerActor.props(0));
            try {
                worker.tell(new RecordMsg(0, 5), probe.getRef());
                worker.tell(new WorkerQueryMsg(reset), probe.getRef());
                worker.tell(new RecordMsg(0, 2), probe.getRef());
                worker.tell(new WorkerQueryMsg(false), probe.getRef());
                WorkerResultMsg first = probe.expectMsgClass(WAIT, WorkerResultMsg.class);
                WorkerResultMsg second = probe.expectMsgClass(WAIT, WorkerResultMsg.class);
                // Separate scheduled sends may arrive in either order.
                WorkerResultMsg original = first.getSum() == 5 ? first : second;
                WorkerResultMsg later = first.getSum() == 5 ? second : first;
                assertEquals(0, original.getWorkerId()); assertEquals(0, later.getWorkerId());
                assertEquals(1, original.getCount()); assertEquals(5, original.getSum());
                assertEquals(reset ? 1 : 2, later.getCount());
                assertEquals(reset ? 2 : 7, later.getSum());
            } finally { system.stop(worker); }
        }
    }
}

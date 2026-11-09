# Distributed Counters — Implementation Plan for Review

This plan follows `assignment.pdf` and the current Java templates. Only this planning document is being added now. All implementation, build configuration changes, tests, README updates, and packaging described below are proposed future work.

## 1. Confirm the structure and build setup

Assignment reference: Overview (page 2), Code (page 8).

- Keep the existing structure: two clients → one coordinator → two workers.
- Preserve package `com.lab.evaluation26`, existing public APIs, client script contents and scheduling, and startup code in `DistributedCounters.java`.
- Complete the TODOs in the three actor classes and add immutable messages where needed.
- Check the available JDK and Maven, and verify that the existing Akka 2.7.0 dependencies support the classic actor APIs used by the templates.
- The Java files currently sit in the project root, while `pom.xml` does not configure a source directory. Plan to configure Maven to compile these root-level sources and keep test sources separate, without moving the supplied files. Configure the Java entry point if needed for a reproducible run command.
- Keep the existing Java 8 source/target compatibility unless the available dependency/toolchain requires a documented adjustment.

Expected outcome: a build that actually compiles the supplied classes, with the original application structure preserved.

## 2. Define the message protocol

Assignment reference: Reports and Reset (page 4), Requirements (page 7).

- Reuse `RecordMsg(key, value)` and client-facing `ReportMsg(reset)` unchanged.
- Add an immutable worker query carrying the reset flag.
- Add an immutable worker result carrying `workerId`, `count`, and `sum`.
- Add an immutable combined report result carrying total `count` and `sum`, with readable output for the client.
- Use `long` for accumulated counts and sums; retain the existing integer record inputs.
- Use `tell(message, self())` for every coordinator command to a worker, including records and report queries.

Expected outcome: each actor has a clear input/output contract; the coordinator only stores temporary aggregation data for its current report.

## 3. Implement records and worker state

Assignment reference: Records and Workers (page 3).

Proposed file: `CounterWorkerActor.java`.

1. Initialize local `count = 0` and `sum = 0`.
2. On every record, increment count and add its value to sum.
3. Treat repeated keys as separate records.
4. Count zero-valued records normally.
5. Rely on the assignment's valid, non-negative input assumption.

Proposed file: `CounterCoordinatorActor.java`.

1. In the ready behavior, route even keys to `workers[0]` and odd keys to `workers[1]`.
2. Keep cumulative record statistics only in the workers.

Expected outcome: routing and local counters match page 3 exactly.

## 4. Implement worker reporting and reset

Assignment reference: Reports and Reset (page 4).

Proposed file: `CounterWorkerActor.java`.

1. When a query arrives, capture the current count and sum in an immutable result and capture the requester actor reference.
2. If reset is true, clear the worker's counters in the same message handler, after capturing their old values.
3. If reset is false, retain the counters.
4. Use the actor system scheduler to send the captured result to the coordinator after a finite simulated processing delay.
5. Do not call `sleep`, block on futures, or read mutable counters from a delayed callback.

Expected outcome: reports return the captured totals, empty workers return `(0, 0)`, and reset clears counters before later records are routed.

## 5. Implement the coordinator's report lifecycle

Assignment reference: Collecting a Report (page 5).

Proposed file: `CounterCoordinatorActor.java`.

1. On a report in the ready behavior, save `getSender()` as the original requester. For an ask request, this is the temporary actor that receives the answer.
2. Initialize temporary totals and track which workers have replied.
3. Switch to a collecting behavior using `getContext().become(...)`, then query both workers with the requested reset flag.
4. While collecting, stash incoming records and client reports. Continue accepting worker results.
5. Accept one result from each worker in either arrival order and add its counters to the temporary totals.
6. Once both results arrive, send exactly one combined result to the saved requester.
7. Clear temporary report state, become ready, and call `unstashAll()`.

Use the existing `AbstractActorWithStash` base class. Process stashed messages in their preserved order; the assignment permits any order for buffered messages. No worker failure recovery or retry protocol is required under the stated assumptions.

Expected outcome: only one report is collected at a time, later records cannot enter the active report, and queued reports retain their original requesters.

## 6. Walk through the assignment's resetting example

Assignment reference: Resetting: An Example (page 6).

Use this exact sequence as an acceptance scenario:

| Coordinator step | Expected behavior |
| --- | --- |
| Before the report | worker0 has `(2, 15)`; worker1 has `(1, 7)` |
| Start `ReportMsg(true)` | Workers capture their old totals and clear their counters |
| Receive `RecordMsg(0, 3)`, then `ReportMsg(false)` during collection | Stash both messages |
| Finish the first report | Reply `(count = 3, sum = 22)` |
| Release the record | worker0 becomes `(1, 3)` |
| Process the queued report | Reply `(count = 1, sum = 3)` and retain these counters |

The order above is the coordinator's processing order. The implementation will preserve the buffered record-before-report order shown here. Results across two independent clients must be assessed from the coordinator's actual processing order, rather than an assumed global client send order.

## 7. Complete asynchronous client requests

Assignment reference: Requirements (page 7), Code (page 8).

Proposed file: the TODO inside `CounterClientActor.sendNext(...)`.

- Send records to the coordinator using `tell`.
- Send reports using the ask pattern with a finite timeout that allows for simulated worker delays and queued reports.
- Handle completion asynchronously and display the result or failure with the client ID.
- Prefer forwarding completion as a message to the client actor, so actor state and output handling stay in its mailbox. Add a receive handler and completion message if needed.
- Keep both supplied scripts, their order, the 500 ms interleave schedule, and startup behavior unchanged.
- Do not use `get`, `join`, `Await`, or sleeps inside actors. The existing sleep in the supplied main method is outside an actor and will be left unchanged.

Expected outcome: clients continue running their scripts while reports are pending, and each response is associated with the requesting client.

## 8. Verify correctness

Use focused automated actor tests for ordering and reset behavior, then run the supplied two-client application. Add test dependencies compatible with classic actors only if needed.

| Check                                             | Expected result |
|---------------------------------------------------| --- |
| Empty report, with and without reset              | `(0, 0)` |
| Even and odd keys                                 | Correct worker receives each record |
| Repeated key and zero value                       | Each record increments count; zero does not change sum |
| Two non-reset reports with no intervening records | Identical totals |
| Reset followed by a report with no new records    | Old totals first, then `(0, 0)` |
| Page 6 reset sequence                             | First `(3, 22)`, then `(1, 3)` |
| Record received while collecting                  | Excluded from current report; processed afterward |
| Second report received while collecuting          | Waits until the active report completes |
| Worker replies arrive in reverse order            | Same combined result; one reply to the client |
| Two clients request reports                       | Each receives its own report result |

- Use controllable test workers through the existing `props(Props, Props)` overload to hold and release replies, proving buffering behavior without relying on timing guesses.
- Verify that workers observe the coordinator as command sender.
- Use test deadlines and message expectations; avoid adding blocking operations to production actors.
- Compile the actual production sources, run the focused tests, and run the unchanged client scripts. Record commands and outcomes.
- Review production code for blocking calls and duplicated cumulative state in the coordinator.

Expected outcome: evidence covers both ordinary counting and the asynchronous boundaries central to the assignment.

## 9. Complete the README

Assignment reference: Rules and Guidelines (page 9).

- Fill in the real group identifier and member names when provided; do not invent them.
- Write at most 200 words describing the implemented message flow: who sends which messages, to whom, when, and how.
- Explain routing, ask requests, worker snapshots and delayed replies, coordinator collection/stashing, and reset behavior.
- Check the description against the finished implementation rather than restating the assignment.

Expected outcome: a submission-ready README once the group details are supplied.

## 10. Prepare the submission package

Assignment reference: Rules and Guidelines (page 9).

- Create `akka-groupXX.zip`, replacing `XX` with the real group identifier.
- Include the source project, build configuration, README, and relevant tests.
- Exclude `target/` output and dependency JARs.
- Inspect the ZIP contents and verify the extracted project can be built using the documented command.
- Leave submission to the member associated with the registered contact email.

Expected outcome: a verified ZIP ready for the registered member to submit.

## Review scope and information still needed

Review this plan before implementation starts. The intended implementation touches the three actor classes, adds message classes and focused tests, makes the minimum necessary Maven configuration adjustments, and completes the README. Existing message APIs, scripts, package name, and startup code remain as supplied.

The group identifier and member names are needed to finalize the README and ZIP name. They do not prevent implementation or correctness checks.

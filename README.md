# Evaluation lab - Akka

## Group number: XX

## Group members

- Student 1
- Student 2
- Student 3

Replace the group number and member placeholders before submission.

## Description of message flows

Clients send records to the coordinator with tell and request reports with ask.
Ask results are piped to each client's mailbox for asynchronous output; failures
are handled there too. The supplied scripts continue on their original schedule.

When ready, the coordinator routes even keys to worker0 and odd keys to worker1,
using itself as sender. Each worker increments its count for every record and
adds the value to its sum, including repeated keys and zero values.

For a report, the coordinator saves the requester, becomes collecting, and sends
both workers a query carrying the reset flag. Each worker captures its counters,
clears them if requested, and schedules its immutable result after 100 ms without
blocking. During collection, the coordinator stashes new records and reports.
It accepts one result per worker in either order, combines them, replies once to
the saved requester, becomes ready, and unstashes messages in FIFO order.
Only workers retain cumulative counters; coordinator totals belong to one report.

## Build and test

Use Maven and a JDK supported by Akka 2.7.0. Sources target Java 8. Tested locally
with JDK 25; Akka emits a deprecated Unsafe API warning on this JDK.

```sh
mvn test
mvn dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=target/runtime-classpath.txt
java -cp "target/classes:$(cat target/runtime-classpath.txt)" com.lab.evaluation26.DistributedCounters
```

The last command is for macOS/Linux. The supplied main keeps the actor system
running after the scripts finish; stop it with Ctrl+C after all seven report
responses and both completion messages appear. No startup code was changed.

The test command works with `-o` when its dependencies are cached. The
dependency classpath command may need network access to download its Maven plugin
dependencies; those are not all cached in the current environment.
See `TEST_SCENARIOS.md` for coverage and expected results.

## Submission

The root archive `akka-groupXX.zip` is a draft with placeholder group details.
Replace those details and rebuild the archive under the real group number before
submitting through the registered contact member. Exclude `target/`, dependency
JARs, IDE files, and existing ZIPs. The message-flow description above is under
200 words; build and submission instructions are separate.

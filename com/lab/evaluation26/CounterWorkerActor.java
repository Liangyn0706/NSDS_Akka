package com.lab.evaluation26;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class CounterWorkerActor extends AbstractActor {
	private final int workerId;

	public CounterWorkerActor(int workerId) {
		this.workerId = workerId;
	}

	public static Props props(int workerId) {
		return Props.create(CounterWorkerActor.class, () -> new CounterWorkerActor(workerId));
	}

	@Override
	public Receive createReceive() {
		// TODO: Complete
		return null;
	}
}

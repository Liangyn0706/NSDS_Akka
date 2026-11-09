package com.lab.evaluation26;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;

public class CounterCoordinatorActor extends AbstractActorWithStash {
	private final Props worker0Props;
	private final Props worker1Props;
	private final ActorRef[] workers = new ActorRef[2];
	
	public CounterCoordinatorActor(Props worker0Props, Props worker1Props) {
		this.worker0Props = worker0Props;
		this.worker1Props = worker1Props;
	}

	public static Props props() {
		return props(CounterWorkerActor.props(0), CounterWorkerActor.props(1));
	}

	public static Props props(Props worker0Props, Props worker1Props) {
		return Props.create(CounterCoordinatorActor.class,
				() -> new CounterCoordinatorActor(worker0Props, worker1Props));
	}

	@Override
	public void preStart() {
		// Create the two workers used by this coordinator
		workers[0] = getContext().actorOf(worker0Props, "worker0");
		workers[1] = getContext().actorOf(worker1Props, "worker1");
	}

	@Override
	public Receive createReceive() {
		// TODO: Complete
		return null;
	}
}

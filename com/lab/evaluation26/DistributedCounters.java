package com.lab.evaluation26;

import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public final class DistributedCounters {
    private DistributedCounters() { }

    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("System");
        ActorRef coordinator = system.actorOf(CounterCoordinatorActor.props(), "coordinator");
        
        ActorRef client0 = system.actorOf(CounterClientActor.props(coordinator, 0), "client0");
        ActorRef client1 = system.actorOf(CounterClientActor.props(coordinator, 1), "client1");

		// Waiting until system is ready
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

        client0.tell(new StartMsg(), ActorRef.noSender());
        client1.tell(new StartMsg(), ActorRef.noSender());
    }
}

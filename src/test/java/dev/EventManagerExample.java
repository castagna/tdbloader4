package dev;

import org.openjena.atlas.event.Event;
import org.openjena.atlas.event.EventListener;
import org.openjena.atlas.event.EventManager;
import org.openjena.atlas.event.EventType;

public class EventManagerExample {

	public static EventType event1 = new EventType("type-01") ;
	public static EventType event2 = new EventType("type-02") ;
	
	public static void main(String[] args) {
		EventTransmitter transmitter = new EventTransmitter();
		new EventReceiver();
		transmitter.transmit();

	}

}

class EventTransmitter {
	public void transmit() {
		Event event1 = new Event(EventManagerExample.event1, null);
		Event event2 = new Event(EventManagerExample.event1, null);
		EventManager.send(null, event1); // null = send to all 
		EventManager.send(null, event2); // null = send to all
	}
}

class EventReceiver {
	public EventReceiver() {
		EventListener listener = new EventListener(){
            @Override
            public void event(Object dest, Event event) {
                System.out.println(dest + ": " + event);
            }
        };
		EventManager.register(null, EventManagerExample.event1, listener);
		EventManager.register(null, EventManagerExample.event2, listener);
	}
	
}



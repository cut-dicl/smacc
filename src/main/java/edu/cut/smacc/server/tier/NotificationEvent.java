package edu.cut.smacc.server.tier;

/**
 * A class that structures a notification event from notification service
 *
 * @author Theodoros Danos
 */
public class NotificationEvent {
    public enum EventType {CREATED, DELETED}

    private EventType event;
    private String bucket;
    private String key;

    NotificationEvent(String bucket, String key, String eventName) {
        this.bucket = bucket;
        this.key = key;
        if (eventName.startsWith("ObjectCreated"))
            event = EventType.CREATED;
        else if (eventName.startsWith("ObjectRemoved"))
            event = EventType.DELETED;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    EventType getEventType() {
        return event;
    }
}
package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@NoArgsConstructor
@Getter
@Setter
public abstract class KafkaResource {
    protected List<Event> events = new ArrayList<Event>();

    public void addResourceEvent(String message, EventType eventType){
        Event evt = new Event();
        evt.date= Date.from(Instant.now());
        evt.message=message;
        evt.eventType=eventType;
        events.add(evt);
    }

    @NoArgsConstructor
    @Getter
    @Setter
    public static class Event {
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        private Date date;
        private String message;
        private EventType eventType;
    }
    public enum EventType {
        REQUESTED,
        AVAILABLE,
        FAILED_RETRYABLE,
        FAILED_UNRECOVERABLE
    }
}

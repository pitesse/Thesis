package com.polimi.f1.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// represents a global track status change from the f1-track-status kafka topic.
// sparse events (only a few per race), broadcast to all flink operators via broadcast state.
// status codes follow the fia standard: "1"=green, "2"=yellow, "4"=sc, "5"=red, "6"=vsc, "7"=vsc ending.
@JsonIgnoreProperties(ignoreUnknown = true)
public class TrackStatusEvent {

    private String date;       // absolute iso-8601 timestamp (session.date + event timedelta)
    private String status;     // fia code: "1", "2", "4", "5", "6", "7"
    private String message;    // human-readable: "AllClear", "Yellow", "SCDeployed", "Red", "VSCDeployed", "VSCEnding"
    private long eventTimeMillis;

    public TrackStatusEvent() {}

    @JsonProperty("Date")
    public String getDate() { return date; }
    @JsonProperty("Date")
    public void setDate(String date) {
        this.date = date;
        this.eventTimeMillis = TelemetryEvent.parseEventTime(date);
    }

    @JsonProperty("Status")
    public String getStatus() { return status; }
    @JsonProperty("Status")
    public void setStatus(String status) { this.status = status; }

    @JsonProperty("Message")
    public String getMessage() { return message; }
    @JsonProperty("Message")
    public void setMessage(String message) { this.message = message; }

    @JsonIgnore
    public long getEventTimeMillis() { return eventTimeMillis; }
    @JsonIgnore
    public void setEventTimeMillis(long eventTimeMillis) { this.eventTimeMillis = eventTimeMillis; }

    @Override
    public String toString() {
        return "TrackStatusEvent{" +
                "date='" + date + '\'' +
                ", status='" + status + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}

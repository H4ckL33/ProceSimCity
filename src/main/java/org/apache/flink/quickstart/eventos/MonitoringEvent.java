package org.apache.flink.quickstart.eventos;

public abstract class MonitoringEvent {

    private int id;

    public MonitoringEvent(int id){
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MonitoringEvent) {
            MonitoringEvent monitoringEvent = (MonitoringEvent) obj;
            return monitoringEvent.canEquals(this) && id == monitoringEvent.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return id;
    }

    public boolean canEquals(Object obj) {
        return obj instanceof MonitoringEvent;
    }

}
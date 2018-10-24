package org.apache.flink.quickstart.eventos;

public abstract class MonitoringEvent {

    private int contador;

    public MonitoringEvent(int contador){
        this.contador = contador;
    }

    public int getContador() {
        return contador;
    }

    public void setContador(int contador) {
        this.contador = contador;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MonitoringEvent) {
            MonitoringEvent monitoringEvent = (MonitoringEvent) obj;
            return monitoringEvent.canEquals(this) && contador == monitoringEvent.contador;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return contador;
    }

    public boolean canEquals(Object obj) {
        return obj instanceof MonitoringEvent;
    }

}
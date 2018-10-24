package org.apache.flink.quickstart.eventos;

public class TemperatureEvent extends MonitoringEvent {

    public TemperatureEvent(int contador, double temperatura) {
        super(contador);
        this.temperatura = temperatura;
    }

    private double temperatura;

    public double getTemperatura() {
        return temperatura;
    }

    public void setTemperatura(double temperatura) {
        this.temperatura = temperatura;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(temperatura);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        TemperatureEvent other = (TemperatureEvent) obj;
        if (Double.doubleToLongBits(temperatura) != Double.doubleToLongBits(other.temperatura))
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "TemperatureEvent [getTemperatura()=" + getTemperatura() + ", getContador()=" + getContador() + "]";
    }

}
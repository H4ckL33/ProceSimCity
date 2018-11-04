package org.apache.flink.quickstart.eventos;

public class RoadEvent extends MonitoringEvent {

    private int mediaTrafico;


    public RoadEvent(int id, int mediaTrafico) {
        super(id);
        this.mediaTrafico = mediaTrafico;
    }

    public int getMediaTrafico() {
        return mediaTrafico;
    }

    public void setMediaTrafico(int mediaTrafico) {
        this.mediaTrafico = mediaTrafico;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(mediaTrafico);
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
        RoadEvent other = (RoadEvent) obj;
        if (mediaTrafico != mediaTrafico)
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "RoadEvent [getMediaTrafico()=" + getMediaTrafico() + ", getId()=" + getId() + "]";
    }

}
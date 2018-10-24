package org.apache.flink.quickstart;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.quickstart.eventos.TemperatureEvent;

public class EventDeserializationSchema implements DeserializationSchema {

    public TypeInformation getProducedType(){
        return TypeExtractor.getForClass(TemperatureEvent.class);
    }

    public TemperatureEvent deserialize(byte[] arg0) throws IOException{
        String str = new String(arg0, StandardCharsets.UTF_8);
        String[] parts = str.split("=");

        return new TemperatureEvent(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    public boolean isEndOfStream(TemperatureEvent arg0){
        return false;
    }

}

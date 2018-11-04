package org.apache.flink.quickstart;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.quickstart.eventos.RoadEvent;

public class EventDeserializationSchema implements DeserializationSchema {

    public TypeInformation getProducedType(){
        return TypeExtractor.getForClass(RoadEvent.class);
    }

    public RoadEvent deserialize(byte[] arg0) throws IOException{
        String str = new String(arg0, StandardCharsets.UTF_8);
        String[] parts = str.split("=");

        return new RoadEvent(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    public boolean isEndOfStream(RoadEvent arg0){
        return false;
    }

}

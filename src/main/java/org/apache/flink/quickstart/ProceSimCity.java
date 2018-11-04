package org.apache.flink.quickstart;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.apache.flink.quickstart.eventos.RoadEvent;
import org.bson.Document;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.quickstart.eventos.Alerta;
import org.apache.flink.quickstart.eventos.MonitoringEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.sql.Timestamp;


public class ProceSimCity {
    public static void main(String[] args) throws Exception{
        //Creamos la conexión para el flujo de datos, y añadimos como semilla de datos un consumidor de kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "roads");

        //Indicamos que vamos a consumir del topic de kafka que se llamará roads
        DataStream<MonitoringEvent> inputEventStream = env.addSource(
                new FlinkKafkaConsumer010<MonitoringEvent>("roads", new EventDeserializationSchema(), properties))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());;


        DataStream<MonitoringEvent> partitionedInput = inputEventStream.keyBy(new KeySelector<MonitoringEvent, Integer>() {
            @Override
            public Integer getKey(MonitoringEvent value) throws Exception {
                return new Integer(value.getId());
            }
        });

        //Definimos las condiciones de alerta
        Pattern<MonitoringEvent, ?> warningPattern = Pattern.<MonitoringEvent> begin("first")
                .subtype(RoadEvent.class).where(new SimpleCondition<RoadEvent>() {
                    @Override
                    public boolean filter(RoadEvent roadEvent) throws Exception {
                        return roadEvent.getMediaTrafico() > 2;
                    }
                }).next("second").subtype(RoadEvent.class).where(new SimpleCondition<RoadEvent>() {
                    @Override
                    public boolean filter(RoadEvent roadEvent) throws Exception {
                        return roadEvent.getMediaTrafico() > 2;
                    }
                }).within(Time.seconds(10));

        Pattern<MonitoringEvent, ?> alertPattern = Pattern.<MonitoringEvent> begin("first")
                .subtype(RoadEvent.class).where(new SimpleCondition<RoadEvent>() {
                    @Override
                    public boolean filter(RoadEvent value) throws Exception {
                        return value.getMediaTrafico() == 4;
                    }
                }).oneOrMore().within(Time.seconds(10));

        //Deteccion de warnings
        PatternStream<MonitoringEvent> patternStream = CEP.pattern(inputEventStream, warningPattern);
        //Deteccion de alerts
        PatternStream<MonitoringEvent> alertPatternStream = CEP.pattern(partitionedInput, warningPattern);
        //Deteccion de polution
        PatternStream<MonitoringEvent> polutionPatternStream = CEP.pattern(partitionedInput, alertPattern);

        DataStream<Alerta> warningStream = patternStream
                .select(new PatternSelectFunction<MonitoringEvent, Alerta>() {
                    private static final long serialVersionUID = 1L;

                    public Alerta select(Map<String, List<MonitoringEvent>> event) throws Exception {
                        Timestamp time = new Timestamp(System.currentTimeMillis());
                        return new Alerta("Aumento de tráfico en la zona. Informar por pantallas, vigilar tramo de carreteras.", 'w',time.toString());
                    }

                });


        DataStream<Alerta> alertStream = alertPatternStream
                .select(new PatternSelectFunction<MonitoringEvent, Alerta>() {
                    private static final long serialVersionUID = 1L;

                    public Alerta select(Map<String, List<MonitoringEvent>> event) throws Exception {
                        RoadEvent evento = (RoadEvent) event.get("first").get(0);
                        String carril = " ";
                        switch (evento.getId()){
                            case 0:carril = "Norte";break;
                            case 1:carril = "Noreste";break;
                            case 2:carril = "Noroeste";break;
                            case 3:carril = "Sudeste";break;
                            case 4:carril = "Suroeste";break;
                        }
                        Timestamp time = new Timestamp(System.currentTimeMillis());
                        return new Alerta("Aumento del tráfico en el carril "+carril+". Informar por pantallas, marcar como tráfico saturado para navegadores. ", 'l', time.toString());
                    }

                });

        DataStream<Alerta> polutionStream = polutionPatternStream
                .select(new PatternSelectFunction<MonitoringEvent, Alerta>() {
                    private static final long serialVersionUID = 1L;

                    public Alerta select(Map<String, List<MonitoringEvent>> event) throws Exception {
                        RoadEvent evento = (RoadEvent) event.get("first").get(0);
                        String carril = " ";
                        switch (evento.getId()){
                            case 0:carril = "Norte";break;
                            case 1:carril = "Noreste";break;
                            case 2:carril = "Noroeste";break;
                            case 3:carril = "Sudeste";break;
                            case 4:carril = "Suroeste";break;
                        }
                        Timestamp time = new Timestamp(System.currentTimeMillis());
                        return new Alerta("Atasco en carril "+carril+". Efectuar analisis de polución.", 'a',time.toString());
                    }

                });


        warningStream.print();
        alertStream.print();
        polutionStream.print();
        partitionedInput.print();

        env.execute("CEP sobre simulador de Smart City");

    }
}

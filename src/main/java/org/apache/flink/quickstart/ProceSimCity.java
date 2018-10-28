package org.apache.flink.quickstart;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.bson.Document;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.quickstart.eventos.Alerta;
import org.apache.flink.quickstart.eventos.MonitoringEvent;
import org.apache.flink.quickstart.eventos.TemperatureEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class ProceSimCity {
    public static void main(String[] args) throws Exception{
        //Creamos la conexión con MongoDB y definimos las colecciones que usaremos
        MongoClient mongo = new MongoClient( "localhost" , 27017 );
        MongoCredential credential;
        credential = MongoCredential.createCredential("developer", "alertEvent", "password".toCharArray());
        MongoDatabase database = mongo.getDatabase("alertEvent");
        MongoCollection<Document> warningCollection = database.getCollection("warningCollection");
        MongoCollection<Document> alertCollection = database.getCollection("alertCollection");
        MongoCollection<Document> accidentCollection = database.getCollection("accidentCollection");
        Document documentoWarnings = new Document("tipo", "warning");
        Document documentoAlerts = new Document("tipo", "alert");
        Document documentoAccidents = new Document("tipo", "accident");


        //Creamos la conexión para el flujo de datos, y añadimos como semilla de datos un consumidor de kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "semaphores");

        //Indicamos que vamos a consumir del topic de kafka que se llamará roads
        DataStream<MonitoringEvent> inputEventStream = env.addSource(
                new FlinkKafkaConsumer010<MonitoringEvent>("semaphores", new EventDeserializationSchema(), properties)).assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());;


        DataStream<MonitoringEvent> partitionedInput = inputEventStream.keyBy(new KeySelector<MonitoringEvent, Integer>() {
            @Override
            public Integer getKey(MonitoringEvent value) throws Exception {
                return new Integer(value.getContador());
            }
        });

        //Definimos las condiciones de alerta
        Pattern<MonitoringEvent, ?> warningPattern = Pattern.<MonitoringEvent> begin("first")
                .subtype(TemperatureEvent.class).where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value) throws Exception {
                        return value.getTemperatura() > 27.0;
                    }
                });

        Pattern<MonitoringEvent, ?> alertPattern = Pattern.<MonitoringEvent> begin("first")
                .subtype(TemperatureEvent.class).where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent) throws Exception {
                        return temperatureEvent.getContador()>40;
                    }
                });

        Pattern<MonitoringEvent, ?> accidentPattern = Pattern.<MonitoringEvent> begin("first")
                .subtype(TemperatureEvent.class).where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value) throws Exception {
                        return value.getContador() > 0;
                    }
                }).next("second").subtype(TemperatureEvent.class).where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent) throws Exception {
                        return temperatureEvent.getContador() > 0;
                    }
                }).within(Time.seconds(5));

        PatternStream<MonitoringEvent> patternStream = CEP.pattern(partitionedInput, warningPattern);

        PatternStream<MonitoringEvent> alertPatternStream = CEP.pattern(partitionedInput, alertPattern);

        PatternStream<MonitoringEvent> accidentPatternStream = CEP.pattern(partitionedInput, accidentPattern);

        DataStream<Alerta> warningStream = patternStream
                .select(new PatternSelectFunction<MonitoringEvent, Alerta>() {
                    private static final long serialVersionUID = 1L;

                    public Alerta select(Map<String, List<MonitoringEvent>> event) throws Exception {
                        TemperatureEvent evento = (TemperatureEvent)event.get("first").get(0);
                        return new Alerta("Temperatura especialmente alta: " + evento.getTemperatura()+ "ºC.", 'w');
                    }

                });


        DataStream<Alerta> alertStream = alertPatternStream
                .select(new PatternSelectFunction<MonitoringEvent, Alerta>() {
                    private static final long serialVersionUID = 1L;

                    public Alerta select(Map<String, List<MonitoringEvent>> event) throws Exception {
                        TemperatureEvent evento = (TemperatureEvent)event.get("first").get(0);
                        //documentoAlerts.append("mensaje", "Acercandose final de tiempo de vida del sensor, cambiar. contador, temperatura: "+evento.getContador()+","+evento.getTemperatura()+".");
                        //alertCollection.insertOne(documentoAlerts);
                        return new Alerta("Acercandose final de tiempo de vida del sensor, cambiar. contador, temperatura: "+evento.getContador()+","+evento.getTemperatura()+".", 'l');
                    }

                });

        DataStream<Alerta> accidentStream = accidentPatternStream
                .select(new PatternSelectFunction<MonitoringEvent, Alerta>() {
                    private static final long serialVersionUID = 1L;

                    public Alerta select(Map<String, List<MonitoringEvent>> event) throws Exception {
                        TemperatureEvent evento = (TemperatureEvent)event.get("first").get(0);
                        //documentoAccidents.append("mensaje", "Fallo del sistema de semáforos, posibilidad de accidente. BLOQUEAR TRAMO DE CARRETERA. contador,temperatura: "+evento.getContador()+","+evento.getTemperatura()+".");
                        //accidentCollection.insertOne(documentoAccidents);
                        return new Alerta("Fallo del sistema de semáforos, posibilidad de accidente. BLOQUEAR TRAMO DE CARRETERA. contador,temperatura: "+evento.getContador()+","+evento.getTemperatura()+".", 'a');
                    }

                });


        warningStream.print();
        alertStream.print();
        accidentStream.print();
        partitionedInput.print();

        env.execute("CEP sobre simulador de Smart City");

    }
}

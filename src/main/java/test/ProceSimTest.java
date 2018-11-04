package test;


import org.apache.flink.quickstart.eventos.Alerta;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.apache.flink.quickstart.eventos.RoadEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class ProceSimTest {
    @Test
    public void testCreateEvent(){
        int contador = 20;
        int mediaTrafico = 2;
        RoadEvent event = new RoadEvent(contador, mediaTrafico);
        assertEquals(event.getId(), contador);
        assertEquals(event.getMediaTrafico(), mediaTrafico);
    }

    @Test
    public void testCreateAlert(){
        String mensaje="Tipo de alerta no válido, revise su creación.";
        char tipo = 'j';
        String timestamp  = "timestamp";
        Alerta alert = new Alerta(mensaje, tipo, timestamp);
        assertEquals(alert.toString(), mensaje);
    }

    @Test
    public void testBdConnection() throws Exception{
        try{
            MongoClient mongo = new MongoClient( "localhost" , 27017 );
            MongoCredential credential = MongoCredential.createCredential("developer", "alertEvent", "password".toCharArray());
            MongoDatabase database = mongo.getDatabase("alertEvent");
        }catch(Exception e){
            System.out.println("Fallo conexión BD");
            e.printStackTrace();
        }
    }

}

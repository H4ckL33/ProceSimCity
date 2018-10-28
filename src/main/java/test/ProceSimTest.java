package test;


import org.apache.flink.quickstart.eventos.Alerta;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.apache.flink.quickstart.eventos.TemperatureEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class ProceSimTest {
    @Test
    public void testCreateEvent(){
        int contador = 20;
        double temperatura = 20.0;
        TemperatureEvent event = new TemperatureEvent(contador, temperatura);
        assertEquals(event.getContador(), contador);
        assertEquals(event.getTemperatura(), temperatura);
    }

    @Test
    public void testCreateAlert(){
        String mensaje="Mensaje de prueba";
        char tipo = 'a';
        Alerta alert = new Alerta(mensaje, tipo);
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

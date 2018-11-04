package org.apache.flink.quickstart.eventos;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.bson.Document;

public class Alerta {

    private String mensaje;
    private String timestamp;
    private char tipo;
    private static MongoClient mongo = new MongoClient( "localhost" , 27017 );
    private static MongoCredential credential = MongoCredential.createCredential("developer", "alertEvent", "password".toCharArray());
    private static MongoDatabase database = mongo.getDatabase("alertEvent");

    public Alerta(String mensaje, char tipo, String timestamp) {
        super();
        this.mensaje = mensaje;
        this.timestamp = timestamp;
        this.tipo = tipo;

        switch (tipo){
            case 'w':
                MongoCollection<Document> warningCollection = database.getCollection("warningsCollection");
                Document documentoWarnings = new Document("tipo", "warning").append("timestamp", this.timestamp).append("mensaje", this.mensaje);
                warningCollection.insertOne(documentoWarnings);break;
            case 'l':
                MongoCollection<Document> alertCollection = database.getCollection("alertsCollection");
                Document documentoAlerts = new Document("tipo", "alert").append("timestamp", this.timestamp).append("mensaje", this.mensaje);
                alertCollection.insertOne(documentoAlerts);break;
            case 'a':
                MongoCollection<Document> criticalCollection = database.getCollection("polutionssCollection");
                Document documentoAccidents = new Document("tipo", "accident").append("timestamp", this.timestamp).append("mensaje", this.mensaje);
                criticalCollection.insertOne(documentoAccidents);break;
            default:
                this.mensaje = "Tipo de alerta no válido, revise su creación.";break;
        }
    }

    public String getMensaje() {
        return mensaje;
    }

    public void setMensaje(String mensaje) {
        this.mensaje = mensaje;
    }

    public char getTipo(){return this.tipo;}

    public void setTipo(char tipo) { this.tipo = tipo; }

    @Override
    public String toString() {
        return "Alert [mensaje=" + mensaje + ", "+timestamp+"]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mensaje == null) ? 0 : mensaje.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Alerta other = (Alerta) obj;
        if (mensaje == null) {
            if (other.mensaje != null)
                return false;
        } else if (!mensaje.equals(other.mensaje))
            return false;
        return true;
    }


}
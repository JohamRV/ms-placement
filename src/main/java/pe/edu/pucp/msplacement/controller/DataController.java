package pe.edu.pucp.msplacement.controller;

import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import java.util.*;

@CrossOrigin
@RestController
@RequestMapping(value="/ms-placement")

public class DataController {
    String urlMongo = "mongodb://localhost:27017";
    int limiteLectura = 80;

    @GetMapping()
    public ResponseEntity prueba(){
        String pruebaData = "API de prueba";
        return new ResponseEntity(pruebaData, HttpStatus.OK);
    }

    @GetMapping(value="worker-status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity chooseWorker2(@RequestParam(value = "sort", required = false, defaultValue = "cpu") String sort){
        //StringBuilder textoColeccion = new StringBuilder(urlMongo);
        try (MongoClient mongoClient = MongoClients.create(urlMongo)) {
            MongoDatabase database = mongoClient.getDatabase("monitoring_db");
            MongoCollection<Document> collection = database.getCollection("compute_data");

            //int limiteLectura = 80;
            //Leer las ultimas entradas según el timestamp de entrada de tiempo
            MongoCursor<Document> cursor = collection.find().sort(Sorts.descending("timestamp")).limit(limiteLectura).iterator();


            // Lista de instancias
            List<String> listaInstancias = new ArrayList<>();

            while (cursor.hasNext()){
                Document document1 = cursor.next();
                String instance1 = document1.getString("instance");
                if(!listaInstancias.contains(instance1)){
                    listaInstancias.add(instance1);
                }
            }

            HashMap<String,Double> promedioUsoCpuInstancias = new HashMap<>();
            HashMap<String,Double> promedioUsoMemoriaInstancias = new HashMap<>();

            for(String nombreInstancia : listaInstancias){
                MongoCursor<Document> cursor2 = collection.find().iterator();
                Double sumaCpu = 0.0;
                Double sumaMemoria = 0.0;
                int cantidad = 0;
                while(cursor2.hasNext()){
                    Document document = cursor2.next();
                    String nombreDBinstance = document.getString("instance");
                    if(nombreInstancia.equals(nombreDBinstance)){
                        Document cpu0 = (Document) document.get("cpu");
                        Document memory0 = (Document) document.get("memory");

                        Double cpu_total = cpu0.getDouble("total");
                        Double memory_percent = memory0.getDouble("UsePercent");

                        sumaCpu = sumaCpu + cpu_total;
                        sumaMemoria = sumaMemoria + memory_percent;
                        cantidad++;
                    }
                }
                Double promedioCpu = sumaCpu/cantidad;
                Double promedioMemoria = sumaMemoria/cantidad;
                promedioUsoCpuInstancias.put(nombreInstancia,promedioCpu);
                promedioUsoMemoriaInstancias.put(nombreInstancia,promedioMemoria);
            }

            // Obtención de los CPU o memoria con menor uso
            HashMap<String,Double> listaIterable;
            if(sort.equals("memory")){
                listaIterable = promedioUsoMemoriaInstancias;
            }else{
                listaIterable = promedioUsoCpuInstancias;
            }

            TreeMap<Double, String> sortedMap = new TreeMap<>();
            for (Map.Entry<String, Double> entry : listaIterable.entrySet()) {
                String clave = entry.getKey();
                double valor = entry.getValue();
                sortedMap.put(valor, clave);
            }

            int contador = 0;
            for (Map.Entry<Double, String> entry : sortedMap.entrySet()) {
                /*
                if(entry.getKey()>20 && entry.getKey()<60){
                    double valor = entry.getKey();
                    String clave = entry.getValue();
                    return new ResponseEntity(sortedMap,HttpStatus.OK);
                }*/
                if (contador >= promedioUsoCpuInstancias.size()) {
                    break;
                }
                contador++;
            }

            return new ResponseEntity(sortedMap,HttpStatus.OK);
        }
    }

}

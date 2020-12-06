package com.alvaro.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class ProductorEventosDeportivos {

    private static final String EXCHANGE = "eventos-deportivos";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        // Abrir conexion AMQ y establecer canal
        try( Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            // Crear topic exchange "eventos-deportivos"
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);
            // pais: es, fr, usa
            List<String> countries = Arrays.asList("es", "fr", "usa");
            // deporte: futbol, tenis, voleibol
            List<String> sports = Arrays.asList("futbol", "tenis", "voleibol");
            // tipo evento: envivo, noticia
            List<String> eventTypes = Arrays.asList("envivo", "noticia");

            int count = 1;
            // Enviar mensajes al topic exchange "eventos-deportivos"
            while (true) {
                shuffle(countries, sports, eventTypes);
                String country = countries.get(0);
                String sport = sports.get(0);
                String eventType = eventTypes.get(0);
                // routing-key -> country.sport.eventType
                String routingKey = country + "." + sport + "." + eventType;

                String message = "Evento " + count;
                System.out.println("Produciendo mensaje (" + country + ", " + sport + ", " + eventType + "): " + message);

                channel.basicPublish(EXCHANGE, routingKey, null, message.getBytes());
                Thread.sleep(1000);
                count++;
            }
        }
    }

    private static void shuffle(List<String> countries, List<String> sports, List<String> eventTypes) {
        Collections.shuffle(countries);
        Collections.shuffle(sports);
        Collections.shuffle(eventTypes);
    }
}

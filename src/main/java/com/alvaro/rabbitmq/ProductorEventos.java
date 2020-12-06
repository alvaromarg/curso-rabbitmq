package com.alvaro.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProductorEventos {

    private static final String EVENTOS = "eventos";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        // Abrir conexion AMQ y establecer canal
        try( Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            // Crear fanout exchange "eventos"
            channel.exchangeDeclare(EVENTOS, BuiltinExchangeType.FANOUT);

            int count = 1;
            // Enviar mensajes al fanout exchange "eventos"
            while (true) {
                String message = "Evento " + count;
                System.out.println("Produciendo mensaje: " + message);
                channel.basicPublish(EVENTOS, "", null, message.getBytes());
                Thread.sleep(1000);
                count++;
            }
        }
    }
}

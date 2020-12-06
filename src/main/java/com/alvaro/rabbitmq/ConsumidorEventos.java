package com.alvaro.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class ConsumidorEventos {

    private static final String EVENTOS = "eventos";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        // Abrir conexion
        Connection connection = connectionFactory.newConnection();
        // Establecer canal
        Channel channel = connection.createChannel();
        // Declarar exchange "eventos"
        channel.exchangeDeclare(EVENTOS, BuiltinExchangeType.FANOUT);
        // Crear cola y asociarla al exchange "eventos"
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EVENTOS, "");
        // Crear subscripcion a una cola asociada al exchange "eventos"
        channel.basicConsume(queueName,
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());
                    System.out.println("Mensaje recibido: " + messageBody);
                },
                consumerTag -> {
                    System.out.println("Consumidor " + consumerTag + " cancelado");
                });
    }
}

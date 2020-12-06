package com.alvaro.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ConsumidorEventosDeportivos {

    private static final String EXCHANGE = "eventos-deportivos";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        // Abrir conexion
        Connection connection = connectionFactory.newConnection();
        // Establecer canal
        Channel channel = connection.createChannel();
        // Declarar exchange "eventos-deportivos"
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);
        // Crear cola y asociarla al exchange "eventos-deportivos"
        String queueName = channel.queueDeclare().getQueue();
        // Patron routing-key -> country.sport.eventType
        // * -> identifica una palabra
        // # -> identifica multiples palabras delimitadas por .
        // eventos tenis -> *.tenis.*
        // eventos en España -> es.# / es.*.*
        // todos los eventos -> #
        System.out.println("Introduzca routing-key: ");
        Scanner scanner = new Scanner(System.in);
        String routingKey = scanner.nextLine();

        channel.queueBind(queueName, EXCHANGE, routingKey);
        // Crear subscripcion a una cola asociada al exchange "eventos-deportivos"
        channel.basicConsume(queueName,
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());
                    System.out.println("Mensaje recibido: " + messageBody);
                    System.out.println("Routing key: " + message.getEnvelope().getRoutingKey());
                },
                consumerTag -> {
                    System.out.println("Consumidor " + consumerTag + " cancelado");
                });
    }
}

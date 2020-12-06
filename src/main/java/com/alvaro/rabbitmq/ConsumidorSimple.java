package com.alvaro.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class ConsumidorSimple {
    public static void main(String[] args) throws IOException, TimeoutException {

        // Abrir conexion AMQ
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        // Establecer canal
        Channel channel = connection.createChannel();

        // Declarar la cola "primera-cola"
        channel.queueDeclare("primera-cola", false, false, false, null);

        // Crear subscripcion a la cola "primera-cola" usando el comando Basic.consume
        channel.basicConsume("primera-cola",
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());

                    System.out.println("Mensaje: " + messageBody);
                    System.out.println("Exchange: " + message.getEnvelope().getExchange());
                    System.out.println("Routing key: " + message.getEnvelope().getRoutingKey());
                    System.out.println("Delivery tag: " + message.getEnvelope().getDeliveryTag());
                },
                consumerTag -> {
                    System.out.println("Consumidor " + consumerTag + " cancelado");
                });
    }
}

package com.github.ansd;

import java.nio.charset.StandardCharsets;
import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import static com.rabbitmq.client.amqp.ConsumerBuilder.StreamOffsetSpecification.FIRST;

public class App {
    public static void main(String[] args) {

        Environment environment = new AmqpEnvironmentBuilder().build();
        Connection connection = environment.connectionBuilder().build();
        Management management = connection.management();
        String streamName = "my-stream";

        management
            .queue()
            .name(streamName)
            .stream()
            .queue()
            .declare();

        Publisher publisher =
            connection
            .publisherBuilder()
            .queue(streamName)
            .build();

        String[] colors = new String[] { "green", "blue", "purple", "purple", "green", "green" };
        for (int i = 0; i < colors.length; i++) {
            String color = colors[i];
            String body = String.format("message %d", i);
            Message msg = publisher
                .message(body.getBytes(StandardCharsets.UTF_8))
                .property("color", color);
            publisher.publish(msg, context -> {});
            System.out.printf("publisher sent %s with color %s\n", body, color);
        }

        consume(connection, streamName, "green");
        consume(connection, streamName, "purple");
        consume(connection, streamName, "blue");
        // Consume all colors with suffix "e" ("blue" and "purple")
        consume(connection, streamName, "&s:e");
    }

    private static void consume(Connection connection, String streamName, String color) {
        ConsumerBuilder builder =
            connection
            .consumerBuilder()
            .queue(streamName)
            .messageHandler(
                    (ctx, msg) -> {
                        System.out.printf(
                                "consumer (filter %s) received %s\n",
                                color,
                                new String(msg.body(), StandardCharsets.UTF_8));
                        ctx.accept();
                    });

        java.util.function.Consumer<ConsumerBuilder.StreamFilterOptions> filterOptions = m -> m.property("color", color);
        filterOptions.accept(builder.stream().offset(FIRST).filter());
        builder.build();
    }
}

package vn.edu.hcmut.vertical;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQOptions;
import vn.edu.hcmut.constant.QueueConstant;

import java.util.concurrent.atomic.AtomicReference;

public class RabbitMQVertical extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUser("hientm177")
      .setPassword("hientm177")
      .setHost("localhost")
      .setTrustAll(true);

    RabbitMQClient client = RabbitMQClient.create(vertx, config);

    AtomicReference<RabbitMQConsumer> consumerRef = new AtomicReference<>();
    AtomicReference<String> queueName = new AtomicReference<>();

    client.addConnectionEstablishedCallback(promise -> {
      System.out.println("Connection to RabbitMQ established successfully");
      client.exchangeDeclare("exchange", "fanout", true, false)
        .compose(v -> client.queueDeclare(QueueConstant.QUEUE1, false, true, true))
        .compose(declareOk -> {
          queueName.set(declareOk.getQueue());
          RabbitMQConsumer currentConsumer = consumerRef.get();
          if (currentConsumer != null) {
            currentConsumer.setQueueName(queueName.get());
          }
          return client.queueBind(queueName.get(), "exchange", "");
        })
        .onComplete(promise);
    });

    client.start()
          .onSuccess(v -> {
            /* Begin here */

            // At this point the exchange, queue and binding will have been declared even if the client connects to a new server
            client.basicConsumer(queueName.get(), rabbitMQConsumerAsyncResult -> {
              if (rabbitMQConsumerAsyncResult.succeeded()) {
                var consumer = rabbitMQConsumerAsyncResult.result();
                consumer.handler( msg -> {
//                    JsonObject json = (JsonObject) msg.body();
                    System.out.println("Got message: " + msg.body());
                  });
                System.out.println("RabbitMQ consumer created !");
                consumerRef.set(consumer);
              }
            });

//            consumer.get().handler(rabbitMQMessage -> {
//              System.out.println("Got message: " + rabbitMQMessage.body().toString());
//            });

          })
          .onFailure(ex -> {
            System.out.println("It went wrong: " + ex.getMessage());
      });

  }
}



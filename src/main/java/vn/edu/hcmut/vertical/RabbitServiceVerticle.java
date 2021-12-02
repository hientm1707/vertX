package vn.edu.hcmut.vertical;


import com.rabbitmq.client.AMQP;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import vn.edu.hcmut.constant.QueueConstant;
import vn.edu.hcmut.interfaces.Calculator;
import vn.edu.hcmut.interfaces.MinusCalculatorImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RabbitMQVerticle extends AbstractVerticle {
  private Message<JsonObject> receivedMessage = null;
  private Map<String, Calculator> map = new ConcurrentHashMap<>();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    map.put("plus", (a, b) -> a + b);
    map.put("minus", new MinusCalculatorImpl());

    //CONFIG
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUser("test")
      .setPassword("test")
      .setHost("172.16.9.166")
      .setPort(5672);



    //GET CLIENT
    RabbitMQClient client = RabbitMQClient.create(vertx, config);



    //Add Callback to Established Connection event
    client.addConnectionEstablishedCallback(promise -> {
      System.out.println("Successfully connected to Rabbit from Service");
    });

    // START THE CLIENT
    client.start()
      .onSuccess(v -> {
        client.basicConsumer(QueueConstant.QUEUE2, rabbitMQConsumerAsyncResult -> {
          if (rabbitMQConsumerAsyncResult.succeeded()) {
            var consumer = rabbitMQConsumerAsyncResult.result();
            consumer.handler(message -> {
                System.out.println("Queue 2 Got message: " + message.body().toJsonObject());
                receivedMessage.reply(message.body());
              }
            );
          }
        });



      })
      .onFailure(ex -> {
        System.out.println("Rabbit  went wrong: " + ex.getCause());
      });

    // Event bus
    EventBus eb = vertx.eventBus();
    MessageConsumer<JsonObject> consumer = eb.consumer(QueueConstant.RABBIT_ADDR);
    consumer.handler(message -> {
      System.out.println("I have received a message: " + message.body());
      basicPublishToQueue(client, message.body().toBuffer(), QueueConstant.QUEUE1, null);
      this.receivedMessage = message;
    });

  }


  public void basicPublishToQueue(RabbitMQClient client, Buffer buffer, String queueConstant, Map<String, Object> headerMap) {
    com.rabbitmq.client.BasicProperties properties = null;
    if (queueConstant == QueueConstant.QUEUE2) {
      properties = new AMQP.BasicProperties()
        .builder()
        .headers(Map.of("h1", "header1")).build();
    } else {
      properties = new AMQP.BasicProperties()
        .builder()
        .headers(headerMap).build();
    }

    client.basicPublish("", queueConstant, buffer, pubResult -> {
      if (pubResult.succeeded()) {
        System.out.println("Message published ! " + buffer.toJsonObject());
      } else {
        pubResult.cause().printStackTrace();
      }
    });
  }


}



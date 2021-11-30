package vn.edu.hcmut.vertical;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQOptions;
import vn.edu.hcmut.constant.QueueConstant;
import vn.edu.hcmut.interfaces.Calculator;
import vn.edu.hcmut.interfaces.MinusCalculatorImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class RabbitMQVerticle extends AbstractVerticle {
  private Message<JsonObject> receivedMessage = null;
  private Map<String, Calculator> map = new ConcurrentHashMap<>();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    map.put("plus", (a, b) -> a + b);
    map.put("minus", new MinusCalculatorImpl());

    //CONFIG
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUser("hientm177")
      .setPassword("hientm177")
      .setHost("localhost")
      .setTrustAll(true);


    //GET CLIENT
    RabbitMQClient client = RabbitMQClient.create(vertx, config);


    AtomicReference<RabbitMQConsumer> consumerRef1 = new AtomicReference<>();

    AtomicReference<RabbitMQConsumer> consumerRef2 = new AtomicReference<>();

    AtomicReference<String> queueName1 = new AtomicReference<>();
    AtomicReference<String> queueName2 = new AtomicReference<>();

    //Add Callback to Established Connection event
    client.addConnectionEstablishedCallback(promise -> {
      System.out.println("Connection to RabbitMQ established successfully");
      client.exchangeDeclare(QueueConstant.EXCHANGE, BuiltinExchangeType.HEADERS.getType(), true, false)
        .compose(v -> client.queueDeclare(QueueConstant.QUEUE1, false, true, true))
        .compose(declareOk -> {
          queueName1.set(declareOk.getQueue());
          RabbitMQConsumer currentConsumer = consumerRef1.get();
          if (currentConsumer != null) {
            currentConsumer.setQueueName(queueName1.get());
          }
          Map<String, Object> args = Map.of(
            "x-match", "any",
            "h1", "header1",
            "h2", "header2"
          );
          return client.queueBind(queueName1.get(), QueueConstant.EXCHANGE, "", args);
        })
        .compose(o -> client.queueDeclare(QueueConstant.QUEUE2, false, true, true))
        .compose(declareOk -> {
          queueName2.set(declareOk.getQueue());
          RabbitMQConsumer currentConsumer = consumerRef2.get();
          if (currentConsumer != null) {
            currentConsumer.setQueueName(queueName2.get());
          }
          Map<String, Object> args = Map.of(
            "x-match", "all",
            "accept", "yest"
          );

          return client.queueBind(queueName2.get(), QueueConstant.EXCHANGE, "",  args);
        })
        .onComplete(promise);
    });

    // START THE CLIENT
    client.start()
      .onSuccess(v -> {
        // Bind queue and create consumer
        client.basicConsumer(queueName1.get(), rabbitMQConsumerAsyncResult -> {
          if (rabbitMQConsumerAsyncResult.succeeded()) {
            var consumer = rabbitMQConsumerAsyncResult.result();
            consumer.handler(message -> {
                System.out.println("Queue1 Got message: " + message.body().toJsonObject());

                var body = message.body().toJsonObject();
                var o1 = body.getInteger("o1");
                var o2 = body.getInteger("o2");
                var op = body.getString("op");

                JsonObject resObj = new JsonObject();
                new Thread( () -> {
                  try {
                    resObj.put("result", map.get(op).calculate(o1, o2));
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  basicPublishToQueue(client, resObj.toBuffer(), QueueConstant.QUEUE2, Map.of(
                    "accept", "yes"
                  ));
                }).start();
              }
            );
            consumerRef1.set(consumer);
          }
        });

        client.basicConsumer(queueName2.get(), rabbitMQConsumerAsyncResult -> {
          if (rabbitMQConsumerAsyncResult.succeeded()) {
            var consumer = rabbitMQConsumerAsyncResult.result();
            consumer.handler(message -> {
                System.out.println("Queue 2 Got message: " + message.body().toJsonObject());
                receivedMessage.reply(message.body());
              }
            );
            consumerRef2.set(consumer);
          }
        });


      })
      .onFailure(ex -> {
        System.out.println("Rabbit  went wrong: " + ex.getCause());
      });


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



package vn.edu.hcmut.vertical;


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

import java.util.concurrent.atomic.AtomicReference;

public class RabbitMQVerticle extends AbstractVerticle {
  private Message<JsonObject> receivedMessage = null;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    //CONFIG
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUser("hientm177")
      .setPassword("hientm177")
      .setHost("localhost")
      .setTrustAll(true);


    //GET CLIENT
    RabbitMQClient client = RabbitMQClient.create(vertx, config);


    AtomicReference<RabbitMQConsumer> consumerRef = new AtomicReference<>();
    AtomicReference<String> queueName = new AtomicReference<>();

    //Add Callback to Established Connection event
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

    // START THE CLIENT
    client.start()
      .onSuccess(v -> {
        // Bind queue and create consumer
        client.basicConsumer(queueName.get(), rabbitMQConsumerAsyncResult -> {
          if (rabbitMQConsumerAsyncResult.succeeded()) {
            var consumer = rabbitMQConsumerAsyncResult.result();
            consumer.handler(message -> {
//                    JsonObject json = (JsonObject) msg.body();
              System.out.println("Got message: " + message.body().toJsonObject());

              var body = message.body().toJsonObject();
              var o1 = body.getInteger("o1");
              var o2 = body.getInteger("o2");
              var op = body.getString("op");

              JsonObject resObj = new JsonObject();
              switch (op) {
                case "plus":
                  resObj.put("result", o1 + o2);
                  break;
                case "minus":
                  resObj.put("result", o1 - o2);
                  break;
              }
//              vertx.eventBus().publish(QueueConstant.WEB_ADDR, resObj.toBuffer());
              receivedMessage.reply(resObj.toBuffer());
            }
            );
            consumerRef.set(consumer);
          }
        });
      })
      .onFailure(ex -> {
        System.out.println("It went wrong: " + ex.getMessage());
      });

// CREATE HTTP ROUTE
//    Router router = Router.router(vertx);
//    router.post("/messages")
//      .handler(routingContext -> {
//        var response = routingContext.response();
//        var request = routingContext.request().bodyHandler(buffer -> {
//          var bufferJO = buffer.toJsonObject();
//          basicPublish(client, buffer);
//          response.putHeader("content-type", "application/json");
//          response.end("haha");
//          System.out.println(bufferJO);
//        });
//
//      });

    EventBus eb = vertx.eventBus();
    MessageConsumer<JsonObject> consumer = eb.consumer(QueueConstant.RABBIT_ADDR);
    consumer.handler(message -> {
      System.out.println("I have received a message: " + message.body());
      basicPublish(client, message.body().toBuffer());
      this.receivedMessage = message;
    });

  }

  ;


//    vertx.createHttpServer()
//      .requestHandler(router)
//      .listen(8080, http -> {
//        if (http.succeeded()) {
//          startPromise.complete();
//          System.out.println("Gateway is ready on port 8080!");
//        } else {
//          startPromise.fail(http.cause());
//        }
//      });



  public void basicPublish(RabbitMQClient client, Buffer buffer) {
//    Buffer message = Buffer.buffer("body", "Hello RabbitMQ, from Vert.x !");
//    JsonObject message = new JsonObject().put();
    client.basicPublish("", QueueConstant.QUEUE1, buffer, pubResult -> {
      if (pubResult.succeeded()) {
        System.out.println("Message published ! " + buffer.toJsonObject());
      } else {
        pubResult.cause().printStackTrace();
      }
    });
  }


}



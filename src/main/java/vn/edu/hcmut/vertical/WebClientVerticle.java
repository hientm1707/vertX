package vn.edu.hcmut.vertical;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.ext.web.Router;
import vn.edu.hcmut.constant.QueueConstant;

public class WebClientVerticle extends AbstractVerticle {


  @Override
  public void start(Promise<Void> startPromise) throws Exception {


    // CREATE HTTP ROUTE
    Router router = Router.router(vertx);
    router.post("/messages")
      .handler(routingContext -> {
        var response = routingContext.response();
        var request = routingContext.request().bodyHandler(buffer -> {
          var bufferJO = buffer.toJsonObject();
          vertx.eventBus().publish(QueueConstant.RABBIT_ADDR, bufferJO);

          MessageConsumer<Buffer> consumer = vertx.eventBus().consumer(QueueConstant.WEB_ADDR);
          consumer.handler(message -> {
            response.putHeader("content-type", "application/json");
            response.end(message.body());
            System.out.println("I have received a message: " + message.body().toJsonObject());
          });
        });
      });

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(8080, http -> {
        if (http.succeeded()) {
          startPromise.complete();
          System.out.println("Gateway is ready on port 8080!");
        } else {
          startPromise.fail(http.cause());
        }
      });
  }


}


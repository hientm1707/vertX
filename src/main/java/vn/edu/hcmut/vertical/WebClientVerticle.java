package vn.edu.hcmut.vertical;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

import io.vertx.ext.web.Router;
import vn.edu.hcmut.constant.QueueConstant;

public class WebClientVerticle extends AbstractVerticle {


  @Override
  public void start(Promise<Void> startPromise) throws Exception {


    // CREATE HTTP ROUTE
    Router router = Router.router(vertx);

    router.post("/calculator/:op")
        .handler(ctx -> {
          var op = ctx.pathParam("op");
          var response = ctx.response();
          ctx.request().bodyHandler(buffer -> {
            var bufferJO = buffer.toJsonObject().put("op", op);
            vertx.eventBus().<Buffer> request(QueueConstant.RABBIT_ADDR, bufferJO)
              .onSuccess(bufferMessage -> {
                  response.putHeader("content-type", "application/json; charset=utf-8");
                  response.end(bufferMessage.body().toString());
                  System.out.println("I have received a message: " + bufferMessage.body().toJsonObject());
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


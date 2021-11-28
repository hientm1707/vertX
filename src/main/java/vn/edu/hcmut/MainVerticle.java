package vn.edu.hcmut;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

public class MainVerticle extends AbstractVerticle {

  public void start(Promise<Void> startPromise) throws Exception {

    vertx.createHttpServer().requestHandler(req -> {
      req.response()
        .putHeader("content-type", "application/json")
        .end("{\"message\": \"Done\"}");
    }).listen(8888, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }
}

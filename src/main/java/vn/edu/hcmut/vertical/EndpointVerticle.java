//package vn.edu.hcmut.vertical;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Promise;
//import io.vertx.ext.web.Router;
//
//public class EndpointVerticle extends AbstractVerticle {
//  @Override
//  public void start(Promise<Void> startPromise)  {
//
//    Router router = Router.router(vertx);
//
//    router.post("/messages")
//      .handler(routingContext -> {
//        var response = routingContext.response();
//        response.putHeader("content-type", "application/json");
//
//        System.out.println(response.end("haha"));
//      });
//
//    vertx.createHttpServer()
//      .requestHandler(router::accept)
//      .listen(8080, http -> {
//        if (http.succeeded()) {
//          startPromise.complete();
//          System.out.println("HTTP server started on port 8080");
//        } else {
//          startPromise.fail(http.cause());
//        }
//      });
//  }
//}

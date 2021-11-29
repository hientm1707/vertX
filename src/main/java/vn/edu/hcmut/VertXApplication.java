package vn.edu.hcmut;

import io.vertx.core.Vertx;
import vn.edu.hcmut.vertical.MainVerticle;
import vn.edu.hcmut.vertical.RabbitMQVertical;

public class VertXApplication {
  public static void main(String [] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle());
    vertx.deployVerticle(new RabbitMQVertical());
  }
}

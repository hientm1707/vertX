package vn.edu.hcmut;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import vn.edu.hcmut.vertical.WebClientVerticle;
import vn.edu.hcmut.vertical.RabbitMQVerticle;

public class VertXApplication {
  public static void main(String [] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new WebClientVerticle());
    vertx.deployVerticle(new RabbitMQVerticle());
  }
}

package dk.sdu.beerfactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Starts the Java backend. Spring wires transport adapters and central production services. */
@SpringBootApplication
@EnableScheduling
public class Application {
  /** Starts Spring Boot and its scheduled coordination tasks. */
  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }
}

package com.miguelmad.cursokafkaspring;


import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@EnableScheduling
@SpringBootApplication
public class CursoKafkaSpringApplication {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private MeterRegistry meterRegistry;

  private static final Logger LOGGER = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

  @KafkaListener(id = "devs4jId", autoStartup = "true", topics = "devs4j-topic", groupId = "devs4j-group", containerFactory = "listenerContainerFactory",
      properties = {"max.poll.intervals.ms:4000", "max.poll.records:50"})
  public void listener(List<ConsumerRecord<String, String>> messages) {
    LOGGER.info("Mensajes recividos ={}",messages.size());
    for (ConsumerRecord<String, String> message : messages) {
     /* LOGGER.info("Offset ={}, Particion={}, ket = {}, value ={}", message.offset(),
          message.partition(), message.key(), message.value());*/
    }
    //LOGGER.info("Finaliza");
  }

  public static void main(String[] args) {
    SpringApplication.run(CursoKafkaSpringApplication.class, args);
  }

  @Scheduled(fixedDelay = 2000, initialDelay = 100)
  public void send() {
    for (int i = 0; i <= 200; i++) {
      kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample Message %d", i));
    }
  }

  @Scheduled(fixedDelay = 2000, initialDelay = 100)
  public void printMetrics() {
    double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
    LOGGER.info("Count {}", count);
  }
}

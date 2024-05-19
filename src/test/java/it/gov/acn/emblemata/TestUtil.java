package it.gov.acn.emblemata;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import it.gov.acn.emblemata.model.Constituency;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import org.slf4j.LoggerFactory;
import org.springframework.data.util.Pair;

public class TestUtil {

  public static Constituency createEnel() {
    return Constituency.builder()
        .name("Enel")
        .address("Via dei salici, XXX Roma(RM)")
        .foundationDate(Instant.now())
        .build();
  }
  public static Constituency createFastweb() {
    return Constituency.builder()
        .name("Fastweb")
        .address("Piazza Grande 12, 00125 Roma(RM)")
        .foundationDate(Instant.now())
        .build();
  }
  public static Constituency createTelecom() {
    return Constituency.builder()
        .name("Telecom")
        .address("Via Ciro Gennaro, 00111 Napoli(NA)")
        .foundationDate(Instant.now())
        .build();
  }
  public static Constituency createShouldNotSeeMe() {
    return Constituency.builder()
        .name("You should not see this")
        .address("Neverland")
        .foundationDate(Instant.now())
        .build();
  }

  public static void changeLogLevels(List<Pair<String, Level>> commands){
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    commands.forEach(pair -> loggerContext.getLogger(pair.getFirst()).setLevel(pair.getSecond()));
  }

  public static Pair<Integer, Integer> getRandomPorsForKafka(){
    // returns a pair of random ports for kafka in the range 49152-65535
    Random random = new Random();
    return Pair.of(random.nextInt(16383)+49152, random.nextInt(16383)+49152);

  }
}

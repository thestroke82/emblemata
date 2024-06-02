package it.gov.acn.emblemata;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import it.gov.acn.emblemata.model.Constituency;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

import org.slf4j.LoggerFactory;
import org.springframework.data.util.Pair;

public class TestUtil {
  private static final String[] names = {"Enel", "Fastweb", "Telecom", "Vodafone", "Wind", "TIM", "Tre", "PosteMobile", "Iliad", "CoopVoce"};
    private static final String[] addresses = {"Via dei salici, XXX Roma(RM)", "Piazza Grande 12, 00125 Roma(RM)", "Via Ciro Gennaro, 00111 Napoli(NA)", "Via dei fiori, 00125 Roma(RM)", "Via dei pini, 00125 Roma(RM)", "Via dei cipressi, 00125 Roma(RM)", "Via dei castagni, 00125 Roma(RM)", "Via dei ciliegi, 00125 Roma(RM)", "Via dei melograni, 00125 Roma(RM)", "Via dei mandarini, 00125 Roma(RM)"};

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

  public static Constituency createRandomConstituency(){
    Random random = new Random();
    return Constituency.builder()
        .name(names[random.nextInt(names.length)])
        .address(addresses[random.nextInt(addresses.length)])
        .foundationDate(Instant.now().minus(random.nextInt(30), ChronoUnit.DAYS))
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

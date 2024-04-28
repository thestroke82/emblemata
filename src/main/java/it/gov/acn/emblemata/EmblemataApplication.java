package it.gov.acn.emblemata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EmblemataApplication {

	public static void main(String[] args) {
		SpringApplication.run(EmblemataApplication.class, args);
	}

}

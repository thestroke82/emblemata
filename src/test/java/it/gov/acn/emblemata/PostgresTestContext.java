package it.gov.acn.emblemata;

import java.util.Optional;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.AuditorAware;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@ActiveProfiles("test")
public class PostgresTestContext {

	protected final static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14.11").withReuse(true);

	static {
		postgres.start();
		if (Boolean.valueOf(System.getenv().getOrDefault("TESTCONTAINERS_RYUK_DISABLED", "false"))) {
			Runtime.getRuntime().addShutdownHook(new Thread(postgres::stop));
		}
	}

	@TestConfiguration
	public static class ContextConfiguration {
		@Bean
		public AuditorAware<String> auditorAwareLocal() {
			return () -> Optional.of("JUnit");
		}
	}

	@DynamicPropertySource
	static void pgProperties(DynamicPropertyRegistry registry) {
		String jdbcUrlPart = postgres.getJdbcUrl();
		String jdbcUrlFull = jdbcUrlPart + "&TC_DAEMON=true";

		registry.add("spring.datasource.url", ()->jdbcUrlFull);
		registry.add("spring.datasource.driver-class-name", org.postgresql.Driver.class::getName);
		registry.add("spring.datasource.username", postgres::getUsername);
		registry.add("spring.datasource.password", postgres::getPassword);
	}



}
package it.gov.acn.emblemata;

import it.gov.acn.emblemata.model.Constituency;

public class Util {

    public static Constituency createEnel() {
        return Constituency.builder()
				.name("Enel")
				.address("Via dei salici, XXX Roma(RM)")
				.build();
    }
    public static Constituency createFastweb() {
        return Constituency.builder()
				.name("Fastweb")
				.address("Piazza Grande 12, 00125 Roma(RM)")
				.build();
    }
    public static Constituency createTelecom() {
        return Constituency.builder()
				.name("Telecom")
				.address("Via Ciro Gennaro, 00111 Napoli(NA)")
				.build();
    }
    public static Constituency createShouldNotSeeMe() {
        return Constituency.builder()
				.name("You should not see this")
				.address("Neverland")
				.build();
    }

}

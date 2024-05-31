package it.gov.acn.emblemata.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;

public class Commons {
    public static final Gson gson = new GsonBuilder()
        .registerTypeAdapter(Instant.class, new InstantGsonTypeAdapter())
        .create();

    public static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    public static String format(Throwable t) {
        if(t == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(t.toString());
        sb.append("  [");
        if(t.getCause()!=null){
            sb.append("Caused by -> ");
            sb.append(t.getCause().toString());
        }
        sb.append("]");
        return sb.toString();
    }
}

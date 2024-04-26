package it.gov.acn.emblemata.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;

public class Commons {
    public static final Gson gson = new GsonBuilder()
        .registerTypeAdapter(Instant.class, new InstantGsonTypeAdapter())
        .create();
}

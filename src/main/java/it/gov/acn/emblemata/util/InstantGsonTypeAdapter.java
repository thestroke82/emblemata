package it.gov.acn.emblemata.util;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Instant;

public class InstantGsonTypeAdapter implements JsonSerializer<Instant>, JsonDeserializer<Instant> {

  @Override
  public JsonElement serialize(Instant instant, Type type,
      JsonSerializationContext jsonSerializationContext) {
    return  instant!=null?new JsonPrimitive(instant.toString()):null;
  }

  @Override
  public Instant deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    return jsonElement!=null?Instant.parse(jsonElement.getAsString()):null;
  }
}

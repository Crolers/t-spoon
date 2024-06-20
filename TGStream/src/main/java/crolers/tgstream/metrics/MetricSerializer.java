package crolers.tgstream.metrics;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.Map;

public class MetricSerializer implements JsonSerializer<Metric> {
    @Override
    public JsonElement serialize(Metric metric, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject serialized = new JsonObject();
        for (Map.Entry<String, Double> entry : metric.toMap().entrySet()) {
            serialized.add(entry.getKey(), new JsonPrimitive(entry.getValue()));
        }
        return serialized;
    }
}

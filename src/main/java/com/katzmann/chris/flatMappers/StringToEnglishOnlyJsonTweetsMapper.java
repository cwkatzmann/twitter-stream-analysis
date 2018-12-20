package com.katzmann.chris.flatMappers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class StringToEnglishOnlyJsonTweetsMapper implements FlatMapFunction<String, JsonNode> {

    @Override
    public void flatMap(String payload, Collector<JsonNode> out) throws IOException {

        ObjectMapper mapper = new ObjectMapper();

        JsonNode node = mapper.readValue(payload, JsonNode.class);

        if (!node.has("delete") && node.get("user").get("lang").asText().equals("en")) {
            out.collect(node);
        }
    }
}

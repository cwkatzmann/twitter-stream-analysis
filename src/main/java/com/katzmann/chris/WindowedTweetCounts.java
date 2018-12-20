package com.katzmann.chris;


import org.apache.flink.api.common.functions.*;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.*;

public class WindowedTweetCounts {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " +
                "[--twitter-consumer-key <consumerKey> --twitter-consumer-secret <consumerSecret>" +
                "--twitter-token <token> --twitter-token-secret <tokenSecret> --parallelism <parallelism>]");


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // set twitter consumer properties
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, params.get("twitter-consumer-key"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, params.get("twitter-consumer-secret"));
        props.setProperty(TwitterSource.TOKEN, params.get("twitter-token"));
        props.setProperty(TwitterSource.TOKEN_SECRET, params.get("twitter-token-secret"));

        DataStream<String> streamSource = env.addSource(new TwitterSource(props));

        SingleOutputStreamOperator<Long> stream = streamSource
                .flatMap(new FlatMapFunction<String, JsonNode>() {

                    @Override
                    public void flatMap(String payload, Collector<JsonNode> out) throws  IOException {

                        ObjectMapper mapper = new ObjectMapper();

                        JsonNode node = mapper.readValue(payload, JsonNode.class);

                        if (!node.has("delete") && node.get("user").get("lang").asText().equals("en")) {
                            out.collect(node);
                        }
                    }
                })
                .broadcast()
                .timeWindowAll(Time.of(15, MINUTES), Time.of(1, MINUTES))
                .aggregate(new AggregateFunction<JsonNode, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JsonNode tweet, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long accumulator1, Long accumulator2) {
                        return accumulator1 + accumulator2;
                    }
                });
        stream.print();

        env.execute();
    }
}
package com.katzmann.chris;

import com.katzmann.chris.dto.TwitterSentiments;
import com.katzmann.chris.flatMappers.StringToEnglishOnlyJsonTweetsMapper;
import com.katzmann.chris.mappers.TweetJsonNodeToTwitterSentimentsMapper;
import com.katzmann.chris.outputFormats.MongoDBOutputFormat;
import com.katzmann.chris.reducers.ReduceTwitterSentiments;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.*;

public class WindowedTwitterPublicSentimentsAggregation {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " +
                "[--twitter-consumer-key <consumerKey> --twitter-consumer-secret <consumerSecret>" +
                "--twitter-token <token> --twitter-token-secret <tokenSecret>" +
                "--mongo-conn-str <mongoConnStr> --mongo-host <mongoHost> --mongo-port <mongoPort>" +
                "--mongo-db <mongoDatabase> --mongo-collection <mongoCollection> --mongo-user <mongoUsername>" +
                "--mongo-pass <mongoPassword>]");


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1   ));

        // set twitter consumer properties
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, params.get("twitter-consumer-key"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, params.get("twitter-consumer-secret"));
        props.setProperty(TwitterSource.TOKEN, params.get("twitter-token"));
        props.setProperty(TwitterSource.TOKEN_SECRET, params.get("twitter-token-secret"));

        DataStream<String> streamSource = env.addSource(new TwitterSource(props));

        SingleOutputStreamOperator<TwitterSentiments> stream = streamSource
                .flatMap(new StringToEnglishOnlyJsonTweetsMapper())
                .keyBy(t -> t.get("user").get("utc_offset")) // key by timezone to allow parallel processing
                .map(new TweetJsonNodeToTwitterSentimentsMapper())
                .timeWindowAll(Time.of(15, MINUTES), Time.of(1, MINUTES))
                .reduce(new ReduceTwitterSentiments());

        MongoDBOutputFormat format;

        MongoDBOutputFormat.MongoDBOutputFormatBuilder builder = MongoDBOutputFormat.builder().
                setCollectionName(params.get("mongoCollection"));

        if (params.get("mongoConnstr") != null) {
            builder = builder
                    .setConnStr(params.get("mongoConnStr"));
        } else {
            builder = builder
                    .setDatbaseName(params.get("mongo-db"))
                    .setHostAddress(params.get("mongo-host"))
                    .setPort(params.getInt("mongo-port"));

            if (params.get("mongo-username") != null) {
                builder = builder
                        .setUsername(params.get("mongo-user"))
                        .setPassword(params.get("mongo-pass"));
            }

            builder = builder.setCollectionName(params.get("mongo-collection"));
        }

        format = builder.finish();

        stream.writeUsingOutputFormat(format);

        env.execute();
    }
}
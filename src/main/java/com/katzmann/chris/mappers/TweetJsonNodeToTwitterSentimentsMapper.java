package com.katzmann.chris.mappers;

import com.katzmann.chris.dto.TwitterSentiments;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.logging.RedwoodConfiguration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.base.CharMatcher;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetJsonNodeToTwitterSentimentsMapper implements MapFunction<JsonNode, TwitterSentiments> {
    /* TweetJsonNodeToTwitterSentimentsMapper is responsible for reading in JsonNodes representing
    tweets (like those returned from polling the Twitter API), and running the Stanford NLP Pipeline
    on them to turn them into TwitterSentiments objects, representing the count of positive, neutral,
    and negative sentiment-labeled sentences in each tweet.
     */

    public TweetJsonNodeToTwitterSentimentsMapper() {
        // set logging level to warn by default before instantiating StanfordCoreNLP pipeline
        RedwoodConfiguration.current().clear().apply();
    }

    @Override
    public TwitterSentiments map(JsonNode tweet) {

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize ssplit pos lemma parse sentiment"); // add "ner" for named entity resolution

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        // convert tweet text to Stanford NLP CoreDocument
        CoreDocument doc = new CoreDocument(tweet.get("text").asText());
        //perform NLP
        pipeline.annotate(doc);

        TwitterSentiments counts = new TwitterSentiments(tweet.get("id").asText());

        for (CoreSentence sentence : doc.sentences()) {

            String sentenceString = sentence.toString();
            if (sentenceString.length() == 0) {
                continue;
            }

            // strip out links and @usernames - CoreNLP appears to bias them towards negative classification.
            // also strip out empty string.
            String regex = "((^|\\s)http(s)?://|(^|\\s)@\\S)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(sentenceString);

            if (matcher.find()) {
                continue;
            }

            // ignore sentences that do not contain all ascii
            boolean isAscii = CharMatcher.ASCII.matchesAllOf(sentenceString);
            if (!isAscii) {
                continue;
            }

            switch (sentence.sentiment().toString()) {
                case "Positive":
                    counts.positive++;
                    break;
                case "Neutral":
                    counts.neutral++;
                    break;
                case "Negative":
                    counts.negative++;
                    break;
            }
        }

        return counts;
    }
}


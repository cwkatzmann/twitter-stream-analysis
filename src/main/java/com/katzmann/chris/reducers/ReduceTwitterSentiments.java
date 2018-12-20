package com.katzmann.chris.reducers;

import com.katzmann.chris.dto.TwitterSentiments;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ReduceTwitterSentiments implements ReduceFunction<TwitterSentiments> {
    @Override
    public TwitterSentiments reduce(TwitterSentiments t2, TwitterSentiments t1) throws Exception {
        t2.positive += t1.positive;
        t2.neutral += t1.neutral;
        t2.negative += t1.negative;

        return t2;
    }
}

package com.katzmann.chris.aggregateFunctions;

import com.katzmann.chris.dto.TwitterSentiments;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateTwitterSentimentsFunction implements
        AggregateFunction<TwitterSentiments, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

    @Override
    public Tuple3<Long, Long, Long> createAccumulator() {
        return new Tuple3<Long, Long, Long>(0L, 0L, 0L);
    }

    @Override
    public Tuple3<Long, Long, Long> add(TwitterSentiments ts, Tuple3<Long, Long, Long> accumulator) {
        return new Tuple3<Long, Long, Long>(
                accumulator.f0 + ts.positive,
                accumulator.f1 + ts.neutral,
                accumulator.f2 + ts.negative
        );
    }

    @Override
    public Tuple3<Long, Long, Long> getResult(Tuple3<Long, Long, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> accumulator1, Tuple3<Long, Long, Long> accumulator2) {
        return new Tuple3<Long, Long, Long>(
                accumulator1.f0 + accumulator2.f0,
                accumulator1.f1 + accumulator2.f1,
                accumulator1.f2 + accumulator2.f1
        );

    }
}

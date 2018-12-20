package com.katzmann.chris.dto;

public class TwitterSentiments {

    public String key;
    public Long positive = 0L;
    public Long neutral = 0L;
    public Long negative = 0L;

    public TwitterSentiments(String key) {
        this.key = key;
    }

    public String toString() {
        return String.format("TwitterSentiments:\nPositive: %d\nNeutral: %d\nNegative: %d\n", positive, neutral, negative);
    }

}

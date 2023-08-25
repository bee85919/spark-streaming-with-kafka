package de.kafka.protocol.generator;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import de.kafka.protocol.event.ClickEvent;
import de.kafka.protocol.event.ImpressionEvent;

public class RandomGenerator {

    private static List<String> AD_IDS = Arrays.asList(
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10)
    );

    public static ImpressionEvent generateImpressionEvent(long timestamp) {
        return new ImpressionEvent(
                RandomStringUtils.randomAlphabetic(10),
                RandomStringUtils.randomAlphabetic(10),
                AD_IDS.get(RandomUtils.nextInt(0, 5)),
                RandomStringUtils.randomAlphabetic(10),
                RandomStringUtils.randomAlphabetic(10),
                RandomStringUtils.randomAlphabetic(10),
                timestamp

        );
    }

    public static ClickEvent generateClickEvent(long timestamp, String impId) {
        return new ClickEvent(
                impId,
                RandomStringUtils.randomAlphabetic(10),
                timestamp
        );
    }
}
package com.abhishek.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.abhishek.models.VideoPopularityStats;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VideoPopularityStatsSerde implements Serde<VideoPopularityStats> {
    
    static ObjectMapper objectMapper = new ObjectMapper();
    private final Serde<VideoPopularityStats> inner;

    public VideoPopularityStatsSerde() {
        this.inner = Serdes.serdeFrom(new VideoPopularityStatsSerializer(), new VideoPopularityStatsDeserializer());
    }

    public static class VideoPopularityStatsSerializer implements Serializer<VideoPopularityStats> {

        @Override
        public byte[] serialize(String topic, VideoPopularityStats data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error serializing JSON message", e);
            }
        }
    }

    public static class VideoPopularityStatsDeserializer implements Deserializer<VideoPopularityStats> {

        @Override
        public VideoPopularityStats deserialize(String topic, byte[] data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.readValue(data, VideoPopularityStats.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error deserializing JSON message", e);
            }
        }
    }

    @Override
    public Serializer<VideoPopularityStats> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<VideoPopularityStats> deserializer() {
        return inner.deserializer();
    }

    public static VideoPopularityStatsSerde get() {
        return new VideoPopularityStatsSerde();
    }

}
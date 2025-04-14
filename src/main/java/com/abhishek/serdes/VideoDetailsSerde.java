package com.abhishek.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.abhishek.models.VideoDetails;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VideoDetailsSerde implements Serde<VideoDetails> {

    static ObjectMapper objectMapper = new ObjectMapper();
    private final Serde<VideoDetails> inner;

    public VideoDetailsSerde() {
        this.inner = Serdes.serdeFrom(new VideoDetailsSerializer(), new VideoDetailsDeserializer());
    }

    public static class VideoDetailsSerializer implements Serializer<VideoDetails> {

        @Override
        public byte[] serialize(String topic, VideoDetails data) {
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

    public static class VideoDetailsDeserializer implements Deserializer<VideoDetails> {

        @Override
        public VideoDetails deserialize(String topic, byte[] data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.readValue(data, VideoDetails.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error deserializing JSON message", e);
            }
        }
    }

    @Override
    public Serializer<VideoDetails> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<VideoDetails> deserializer() {
        return inner.deserializer();
    }

    public static VideoDetailsSerde get() {
        return new VideoDetailsSerde();
    }
}
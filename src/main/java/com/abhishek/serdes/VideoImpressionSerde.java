package com.abhishek.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.abhishek.models.VideoImpression;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VideoImpressionSerde implements Serde<VideoImpression>{

        static ObjectMapper objectMapper = new ObjectMapper();
        private final Serde<VideoImpression> inner;

        public VideoImpressionSerde() {
            this.inner = Serdes.serdeFrom(new VideoImpressionSerializer(), new VideoImpressionDeserializer());
        }

        public static class VideoImpressionSerializer implements Serializer<VideoImpression> {

            @Override
            public byte[] serialize(String topic, VideoImpression data) {
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

        public static class VideoImpressionDeserializer implements Deserializer<VideoImpression> {

            @Override
            public VideoImpression deserialize(String topic, byte[] data) {
                try {
                    if (data == null) {
                        return null;
                    }
                    return objectMapper.readValue(data, VideoImpression.class);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Error deserializing JSON message", e);
                }
            }
        }

        @Override
        public Serializer<VideoImpression> serializer() {
            return inner.serializer();
        }

        @Override
        public Deserializer<VideoImpression> deserializer() {
            return inner.deserializer();
        }

        public static VideoImpressionSerde get() {
            return new VideoImpressionSerde();
        }
    
}

package com.abhishek.kstream;

import java.nio.Buffer;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.springframework.stereotype.Component;

import com.abhishek.models.VideoDetails;
import com.abhishek.models.VideoImpression;
import com.abhishek.models.VideoPopularityStats;
import com.abhishek.serdes.VideoDetailsSerde;
import com.abhishek.serdes.VideoImpressionSerde;
import com.abhishek.serdes.VideoPopularityStatsSerde;

import jakarta.annotation.PostConstruct;

@Component
public class PopularityGraphStream {

    @PostConstruct
    public void init() {
        
        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put("application.id", "popularity-graph-app4");
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/popularity-graph-app");

        String videoDetailsTopic = "video-details";
        String videoImpressionTopic = "video-impression";
        String videoPopularityStatsTopic = "video-popularity-stats";

        GlobalKTable<String, VideoDetails> videoDetailsTable = builder.globalTable(videoDetailsTopic, 
            Consumed.with(Serdes.String(), VideoDetailsSerde.get()));

        KStream<String, VideoImpression> videoImpressionStream = builder.stream(videoImpressionTopic, 
            Consumed.with(Serdes.String(), VideoImpressionSerde.get()));

        KStream<String, VideoImpression> joinedStreamWithVideoDetails = videoImpressionStream
            .join(videoDetailsTable, (k, v) -> v.getVideoId(), (k, v1, v2) -> {
                // Join logic here
                v1.setVideoTitle(v2.getVideoTitle());
                v1.setVideoLengthSeconds(v2.getVideoLengthSeconds());
                return v1;
            });
        
        KTable<String, VideoPopularityStats> vTable =  joinedStreamWithVideoDetails
            .groupBy((k,v) -> v.getVideoId(), Grouped.with(Serdes.String(), VideoImpressionSerde.get()))
            .aggregate(VideoPopularityStats::new, (k, v, agg) -> {
                agg.setVideoId(v.getVideoId());
                agg.setVideoTitle(v.getVideoTitle());
                agg.setVideoLengthSeconds(v.getVideoLengthSeconds());
                
                if(agg.getVideoPopularityStats() == null) {
                    agg.setVideoPopularityStats(new Integer[v.getVideoLengthSeconds()]);
                }

                for(int i = v.getVideoStartTimeSec(); i <= v.getVideoEndTimeSec(); i++) {
                    if(agg.getVideoPopularityStats()[i] == null) {
                        agg.getVideoPopularityStats()[i] = 0;
                    }else{
                        agg.getVideoPopularityStats()[i] += 1;
                    }
                }
                return agg;
            }, Materialized.with(Serdes.String(), VideoPopularityStatsSerde.get()));

        vTable.toStream().to(videoPopularityStatsTopic, Produced.with(Serdes.String(), VideoPopularityStatsSerde.get()));
        
        
        KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.start();

        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));

    }
    
}

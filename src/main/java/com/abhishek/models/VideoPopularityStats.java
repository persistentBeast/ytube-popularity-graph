package com.abhishek.models;

import lombok.Data;

@Data
public class VideoPopularityStats {

    private String videoId;
    private String videoTitle;
    Integer[] videoPopularityStats;
    private int videoLengthSeconds;    
}

package com.abhishek.models;

import lombok.Data;

@Data
public class VideoImpression {

    private String videoId;
    private String userId;
    private int videoStartTimeSec;
    private int videoEndTimeSec;
    private int videoLengthSeconds;
    private String videoTitle;
    
}

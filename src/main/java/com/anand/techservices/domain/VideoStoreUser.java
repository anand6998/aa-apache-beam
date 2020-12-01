package com.anand.techservices.domain;

import lombok.*;

import java.io.Serializable;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class VideoStoreUser implements Serializable {
    private String sid;
    private String uid;
    private String uName;
    private String vid;
    private String duration;
    private String startTime;
    private String sex;

}

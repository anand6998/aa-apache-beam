package com.anand.movieratings.domain;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MovieRating implements Serializable {

    //user_id | item_id | rating | timestamp
    Integer userId;
    Integer movieId;
    Integer rating;



}

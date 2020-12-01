package com.anand.movieratings.domain;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MappedMovieObject implements Serializable {
    Integer movieId;
    String movieTitle;
    Integer userId;
    Integer rating;


}

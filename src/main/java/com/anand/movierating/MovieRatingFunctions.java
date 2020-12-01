package com.anand.movierating;

import com.anand.movieratings.domain.MappedMovieObject;
import com.anand.movieratings.domain.MovieDetails;
import com.anand.movieratings.domain.MovieRating;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

class MapMovieRatingFunction extends DoFn<String, MovieRating> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String[] arr = ctx.element().split("\t");
        final MovieRating movieRating = new MovieRating(
                Integer.parseInt(arr[0]), //userId
                Integer.parseInt(arr[1]), //movieId
                Integer.parseInt(arr[2]) //rating
        );
        ctx.output(movieRating);
    }
}

class ReadAndMapMovieDetailsFunction extends DoFn<String, MovieDetails> {

    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("d-MMM-yyyy");

    public static LocalDate parseDate(String value) {
        if (Objects.isNull(value) || value.trim().equals("")) {
            return null;
        }

        return LocalDate.parse(value, dateTimeFormatter);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String[] arr = ctx.element().split("\\|");
        final Integer movieId = Integer.parseInt(arr[0]);
        final String movieTitle = arr[1];
        final LocalDate releaseDt = parseDate(arr[2]);
        java.sql.Date releaseDate = null;
        if (Objects.nonNull(releaseDt)) {
            releaseDate = java.sql.Date.valueOf(releaseDt);
        } else {
            releaseDate = null;
        }

        final MovieDetails.Builder builder = new MovieDetails.Builder()
                .setMovieId(movieId)
                .setMovieTitle(movieTitle)
                .setReleaseDate(releaseDate);

        MovieDetails movieDetails = builder.build();
        ctx.output(movieDetails);

    }
}

class MapMovieDetailsFunction3 extends DoFn<KV<Integer, Double>, Row> {

    RedissonClient redissonClient;
    Schema rowSchema;

    public MapMovieDetailsFunction3(Schema schema) {
        this.rowSchema = schema;
    }

    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) {
        //System.out.println("Starting bundle");
        final PipelineOptions options = startBundleContext.getPipelineOptions();
        final RedisOptions redisOptions = options.as(RedisOptions.class);
        final RedissonClient redissonClient = redisOptions.getRedissonClient();
        this.redissonClient = redissonClient;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {

        final Integer movieId = ctx.element().getKey();
        final RBucket<MovieDetails> movieDetailsRBucket = redissonClient.getBucket("movie_details:" + movieId);
        final MovieDetails movieDetails = movieDetailsRBucket.get();

        String movieTitle = "N/A";
        if (Objects.nonNull(movieDetails)) {
            movieTitle = movieDetails.getMovieTitle();
        }

        final Double  avgRating = ctx.element().getValue();

        final Row row = Row.withSchema(this.rowSchema).addValues(movieId, movieTitle, avgRating).build();
        ctx.output(row);

    }

    @Teardown
    public void teardown() {
        if (!this.redissonClient.isShutdown()) {
            //System.out.println("Shutting down redisson client");
            this.redissonClient.shutdown();
        }
    }

}

class MapMovieDetailsFunction2 extends DoFn<MovieRating, MappedMovieObject> {

    RedissonClient redissonClient;

    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) {
        System.out.println("Starting bundle");
        final PipelineOptions options = startBundleContext.getPipelineOptions();
        final RedisOptions redisOptions = options.as(RedisOptions.class);
        final RedissonClient redissonClient = redisOptions.getRedissonClient();
        this.redissonClient = redissonClient;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {

        final MovieRating movieRating = ctx.element();
        final RBucket<MovieDetails> movieDetailsRBucket = redissonClient.getBucket("movie_details:" + movieRating.getMovieId());

        final Integer movieId = movieRating.getMovieId();
        final MovieDetails movieDetails = movieDetailsRBucket.get();

        String movieTitle = "N/A";
        if (Objects.nonNull(movieDetails)) {
            movieTitle = movieDetails.getMovieTitle();
        }

        final Integer userId = movieRating.getUserId();
        final Integer rating = movieRating.getRating();

        final MappedMovieObject mappedMovieObject = new MappedMovieObject(movieId, movieTitle, userId, rating);
        ctx.output(mappedMovieObject);

    }

    @Teardown
    public void teardown() {
        if (!this.redissonClient.isShutdown()) {
            System.out.println("Shutting down redisson client");
            this.redissonClient.shutdown();
        }
    }

}


class MapMovieDetailsFunction extends DoFn<MovieRating, MappedMovieObject> {
    private static RedissonClient redisClient;
    private final static Object lock = new Object();
    private String redisHost;
    private int redisPort;


    public MapMovieDetailsFunction(String redisHost, Integer redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    private RedissonClient getRedisClient() {

        if (redisClient == null) {
            synchronized (lock) {
                if (redisClient == null) {
                    Config config = new Config();
                    config.useSingleServer()
                            .setAddress("redis://" + redisHost + ":" + redisPort);
                    redisClient = Redisson.create(config);
                }
            }
        }
        return redisClient;

    }

    @ProcessElement
    public void processElement(ProcessContext ctx, PipelineOptions options) {
        final RedissonClient redissonClient = getRedisClient();

        final MovieRating movieRating = ctx.element();

        final RBucket<MovieDetails> movieDetailsRBucket = redissonClient.getBucket("movie_details:" + movieRating.getMovieId());

        final Integer movieId = movieRating.getMovieId();
        final MovieDetails movieDetails = movieDetailsRBucket.get();

        String movieTitle = "N/A";
        if (Objects.nonNull(movieDetails)) {
            movieTitle = movieDetails.getMovieTitle();
        }

        final Integer userId = movieRating.getUserId();
        final Integer rating = movieRating.getRating();

        final MappedMovieObject mappedMovieObject = new MappedMovieObject(movieId, movieTitle, userId, rating);
        ctx.output(mappedMovieObject);

    }

    @Teardown
    public void shutdown() {
        System.out.println("Shutting down redis client");
        if (Objects.nonNull(redisClient)) {
            if (!redisClient.isShutdown()) {
                redisClient.shutdown();
            }
        }
    }
}


@Component
public class MovieRatingFunctions implements Serializable {

    public void testMappingFunction(String[] args) throws Exception {

        final String redisHost = "redis_vm";
        final Integer redisPort = 6379;


        final RedisOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RedisOptions.class);
        final Pipeline p = Pipeline.create(options);

        final MapMovieDetailsFunction2 mapMovieDetailsFunction = new MapMovieDetailsFunction2();
        //final MapMovieDetailsFunction mapMovieDetailsFunction = new MapMovieDetailsFunction(redisHost, redisPort);

        final PCollection<MovieRating>
                movieRatingPCollection =
                p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/movie_ratings/u.data"))
                        .apply(ParDo.of(new MapMovieRatingFunction()));

        final PCollection<MovieDetails>
                movieDetailsPCollection = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/movie_ratings/u.item"))
                .apply(ParDo.of(new ReadAndMapMovieDetailsFunction()));

        final PCollection<KV<Integer, String>> movieDetailsKVCollection
                = movieDetailsPCollection.apply(ParDo.of(new DoFn<MovieDetails, KV<Integer, String>>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                ctx.output(KV.of(ctx.element().getMovieId(), ctx.element().getMovieTitle()));
            }
        }));

        final PCollectionView<Map<Integer, String>> movieDetailsCollectionView = movieDetailsKVCollection.apply(View.<Integer, String>asMap());

        final PCollection<MappedMovieObject> mappedMovieObjectPCollection
                = movieRatingPCollection.apply(ParDo.of(mapMovieDetailsFunction));

        final PCollection<KV<Integer, Integer>> movieRatingsKVCollection
                = movieRatingPCollection.apply(ParDo.of(new DoFn<MovieRating, KV<Integer, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                ctx.output(KV.of(ctx.element().getMovieId(), ctx.element().getRating()));
            }

        }));

        final PCollection<KV<Integer, Double>> averageMovieRatings = movieRatingsKVCollection.apply(Mean.<Integer, Integer>perKey());

        final Schema schema = Schema.builder()
                .addInt32Field("movieId")
                .addStringField("movieTitle")
                .addDoubleField("movieRating")
                .build();

        final PCollection<Row> movieRtngsRowsCollection = averageMovieRatings.apply(ParDo.of(new DoFn<KV<Integer, Double>, Row>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                final Map<Integer, String> movieDetailsMap = ctx.sideInput(movieDetailsCollectionView);

                final KV<Integer, Double> kvPair = ctx.element();
                final Integer movieId = kvPair.getKey();
                final String movieTitle = movieDetailsMap.get(movieId);
                final Double avgRating = kvPair.getValue();

                final Row movieRating = Row.withSchema(schema).addValues(movieId, movieTitle, avgRating).build();
                ctx.output(movieRating);
            }
        }).withSideInputs(movieDetailsCollectionView));

        final PCollection<Row> movieRtngsRowsCollection2
                = averageMovieRatings.apply(ParDo.of(new MapMovieDetailsFunction3(schema)));


        movieRtngsRowsCollection.setRowSchema(schema);
        movieRtngsRowsCollection2.setRowSchema(schema);

        movieRtngsRowsCollection2.apply(ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                System.out.println(ctx.element());
            }
        }));


//        mappedMovieObjectStrCollection.apply(TextIO.write().to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/mapped_movie_objects")
//                .withNumShards(1).withSuffix(".csv"));
//        client.shutdown();

        p.run();
    }
}

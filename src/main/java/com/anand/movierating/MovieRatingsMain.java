package com.anand.movierating;

import com.google.common.base.Stopwatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.TimeUnit;

public class MovieRatingsMain {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");
        final MovieRatingFunctions movieRatingFunctions = (MovieRatingFunctions) ctx.getBean("movieRatingFunctions");

        Stopwatch timer = Stopwatch.createStarted();

        movieRatingFunctions.testMappingFunction(args);
        long timeUsed = timer.elapsed(TimeUnit.MILLISECONDS);
        //timer.stop();

        System.out.println("timeUsed: " + timeUsed);
    }
}

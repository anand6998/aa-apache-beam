package com.anand.movierating;

import com.anand.movieratings.domain.MappedMovieObject;
import org.apache.beam.sdk.transforms.Combine;

public class AverageFn extends Combine.CombineFn<MappedMovieObject, AverageFn.Accum, Double> {
    public static class Accum {
        int sum = 0;
        int count = 0;
    }

    @Override
    public Accum createAccumulator() { return new Accum(); }

    @Override
    public Accum addInput(Accum accum, MappedMovieObject input) {
        accum.sum += input.getRating();
        accum.count++;
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.sum += accum.sum;
            merged.count += accum.count;
        }
        return merged;
    }

    @Override
    public Double extractOutput(Accum accum) {
        return ((double) accum.sum) / accum.count;
    }
}
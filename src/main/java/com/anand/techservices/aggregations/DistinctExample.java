package com.anand.techservices.aggregations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pCustList = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/Distinct.csv"));
        PCollection<String> uniqueCustList = pCustList.apply(
                Distinct.<String>create()
        );

        uniqueCustList.apply(
                TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/distinct_out.csv")
                .withSuffix(".csv").withNumShards(1)
        );
        p.run();

    }
}

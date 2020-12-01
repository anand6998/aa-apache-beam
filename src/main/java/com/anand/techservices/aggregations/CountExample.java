package com.anand.techservices.aggregations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pCustList = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/Distinct.csv"));

        PCollection<Long> pLong = pCustList.apply(Count.globally());
        pLong.apply(ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void processElement(ProcessContext ctx)  {
                System.out.println( ctx.element());
            }
        }));
        p.run();

    }
}

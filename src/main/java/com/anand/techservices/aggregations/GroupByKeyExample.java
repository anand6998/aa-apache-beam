package com.anand.techservices.aggregations;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class StringToKV extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String input = ctx.element();
        String[] arr = input.split(",");

        ctx.output(KV.of(arr[0], Integer.valueOf(arr[3])));
    }
}

class KVToString extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String key = ctx.element().getKey();
        Iterable<Integer> values = ctx.element().getValue();

        int sum = 0;
        for (Integer i : values) {
            sum += i;
        }

        ctx.output(key + ", " + String.valueOf(sum));


    }
}

public class GroupByKeyExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> pCustOrderList = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/GroupByKey_data.csv"));

        //Step 2 - Convert String to KV
        PCollection<KV<String, Integer>> kvOrderCollection = pCustOrderList.apply(ParDo.of(new StringToKV()));

        //Step 3 - Apply groupByKey
        PCollection<KV<String, Iterable<Integer>>> kvOrderGroupByCollection = kvOrderCollection.apply(GroupByKey.<String, Integer>create());


        //Step 4 - Convert KV<String, Iterable<Integer> to string
        PCollection<String> output = kvOrderGroupByCollection.apply(ParDo.of(new KVToString()));

        //Step 5 - output to file
        output.apply(
                TextIO.write()
                        .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/group_by_example_output.csv")
                        .withSuffix(".csv").withNumShards(1)
        );

        p.run();

    }
}

package com.anand.techservices.join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;


public class LeftJoinExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        //Step 1 - create KV Object
        PCollection<KV<String, String>> pOrderCollection = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/join/user_order.csv"))
                .apply(ParDo.of(new OrderParsingFunction()));

        PCollection<KV<String, String>> pUserCollection = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/join/p_user.csv"))
                .apply(ParDo.of(new UserParsingFunction()));

        //Step 2 - create TupleTag object
        final TupleTag<String> orderTuple = new TupleTag<>();
        final TupleTag<String> userTuple = new TupleTag<>();

        //Step 3 - combine data sets

        PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(orderTuple, pOrderCollection)
                .and(userTuple, pUserCollection)
                .apply(CoGroupByKey.<String>create());

        //Step 4 - Iterate and build String
        PCollection<String> output = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext ctx ) {
                String key = ctx.element().getKey();
                CoGbkResult valObject = ctx.element().getValue();

                Iterable<String> orderTable = valObject.getAll(orderTuple);
                Iterable<String> userTable = valObject.getAll(userTuple);

                for (String order: orderTable) {
                    if (userTable.iterator().hasNext()) {
                        for (String user: userTable) {
                            ctx.output(key + ", " + order + ", " + user);
                        }
                    } else {
                        ctx.output(key + ", " + order + ", " + null);
                    }
                }

            }
        }));

        //Step 5 - write to file
        output.apply(TextIO.write().to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/left_join_example.csv").withNumShards(1).withSuffix(".csv"));

        p.run();

    }
}

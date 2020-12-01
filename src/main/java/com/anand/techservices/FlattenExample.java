package com.anand.techservices;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

public class FlattenExample {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");

        Resource resource1 = ctx.getResource("classpath:/input/customer_1.csv");
        Resource resource2 = ctx.getResource("classpath:/input/customer_2.csv");
        Resource resource3 = ctx.getResource("classpath:/input/customer_3.csv");
        Pipeline p = Pipeline.create();

        PCollection<String> custList1 = p.apply(TextIO.read().from(resource1.getFile().getAbsolutePath()));
        PCollection<String> custList2 = p.apply(TextIO.read().from(resource2.getFile().getAbsolutePath()));
        PCollection<String> custList3 = p.apply(TextIO.read().from(resource3.getFile().getAbsolutePath()));

        PCollectionList<String> mergedCollections = PCollectionList.of(custList1).and(custList2).and(custList3);
        PCollection<String> merged = mergedCollections.apply(Flatten.pCollections());

        merged.apply(TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/merged_out.csv")
                .withSuffix(".csv")
                .withHeader("Id,Name,LastName,City")
                .withNumShards(1));
        p.run();

    }
}

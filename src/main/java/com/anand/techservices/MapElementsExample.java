package com.anand.techservices;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

import java.io.File;


public class MapElementsExample {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");

        Resource resource = ctx.getResource("classpath:/customer.csv");
        Pipeline p = Pipeline.create();
        PCollection<String> pCustList
                = p.apply(
                        TextIO.read().from(resource.getFile().getAbsolutePath()));

        ProcessFunction<String, String> mapFunction = (ProcessFunction<String, String>) input -> input.toUpperCase();

        PCollection<String> pOutputCustList = pCustList.apply(
                MapElements.into(TypeDescriptors.strings())
                .via(mapFunction));



        pOutputCustList.apply(TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/customer_out.csv")
        .withSuffix(".csv")
        .withNumShards(1));

        System.out.println(pCustList);
        p.run();
    }
}

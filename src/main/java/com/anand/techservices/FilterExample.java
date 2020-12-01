package com.anand.techservices;


import com.anand.techservices.domain.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

import java.io.IOException;

class CustomerEntityFilterFunction extends DoFn<CustomerEntity, CustomerEntity> {
   @ProcessElement
    public void processElement(ProcessContext ctx) {
       CustomerEntity entity = ctx.element();
       if (entity.getCity().equals("Los Angeles")) {
           ctx.output(entity);
       }
   }
}

class FilterHeaderFunction extends DoFn<String, String> {
    private final String headerFilter;

    public FilterHeaderFunction(String headerFilter) {
        this.headerFilter = headerFilter;

    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        final String row = ctx.element();
        if (!row.equals(headerFilter)) {
            ctx.output(row);
        }
    }

}

class CustomerEntityMappingFunction extends DoFn<String, CustomerEntity> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {

        final String arr[] = ctx.element().split(",");
        CustomerEntity entity = new CustomerEntity(
                Integer.valueOf(arr[0]),
                arr[1],
                arr[2],
                arr[3]
        );
        ctx.output(entity);
    }
}

class CustomerEntityToStringMappingFunction extends DoFn<CustomerEntity, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        CustomerEntity entity = ctx.element();
        ctx.output(entity.toString());
    }
}

public class FilterExample {
    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");

        Resource resource = ctx.getResource("classpath:/input/customer_pardo.csv");
        Pipeline p = Pipeline.create();

        PCollection<String> pCollection = p.apply(TextIO.read().from(resource.getFile().getAbsolutePath()));
        final String header = "Id,Name,LastName,City";

        PCollection<String> vals = pCollection.apply(ParDo.of(new FilterHeaderFunction(header)));
        PCollection<String> pOutput = vals
                .apply(ParDo.of(new CustomerEntityMappingFunction())) // Map input string to objects
                .apply(ParDo.of(new CustomerEntityFilterFunction()))  // Filter objects on some condition
                .apply(ParDo.of(new CustomerEntityToStringMappingFunction())); // Convert objects to strings for output

        pOutput.apply(TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/cust_output_filter")
                .withNumShards(1)
                .withSuffix(".csv"));
        p.run();

    }
}

package com.anand.techservices;


import com.anand.techservices.domain.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

import java.io.IOException;

class CustFilter extends DoFn<String, CustomerEntity> {

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        String line = processContext.element();
        String[] arr = line.split(",");

        if (arr[3].equals("Los Angeles")) {
            CustomerEntity customerEntity = new CustomerEntity(
                    Integer.valueOf(arr[0]),
                    arr[1],
                    arr[2],
                    arr[3]
            );
            processContext.output(customerEntity);
        }
    }

}
public class ParDoExample {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");

        Resource resource = ctx.getResource("classpath:/customer_pardo.csv");
        Pipeline p = Pipeline.create();

        PCollection<String> pCustList = p.apply(TextIO.read().from(resource.getFile().getAbsolutePath()));
        PCollection<CustomerEntity> customerEntityPCollection = pCustList.apply(ParDo.of(new CustFilter()));

        PCollection<String> output =
                customerEntityPCollection.apply(MapElements.into(TypeDescriptors.strings())
                .via((CustomerEntity entity) -> entity.toString()));

        output.apply(
                TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/pardo_example.csv")
                        .withHeader("Id, Name, LastName, City")
                .withSuffix(".csv").withNumShards(1)
        );
        p.run();

    }
}

package com.anand.techservices;

import com.anand.techservices.domain.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

import java.io.IOException;

class CityBasedPartitionFunction implements Partition.PartitionFn<CustomerEntity> {
    @Override
    public int partitionFor(CustomerEntity elem, int numPartitions) {
        switch (elem.getCity()) {
            case "Los Angeles":
                return 0;

            case "Phoenix":
                return 1;

            default:
                return 2;
        }
    }
}

public class PartitionExample {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");

        Resource resource = ctx.getResource("classpath:/Partition.csv");
        Pipeline p = Pipeline.create();

        PCollection<String> pCollection = p.apply(TextIO.read().from(resource.getFile().getAbsolutePath()));
        final String header = "Id,Name,LastName,City";

        PCollection<String> vals = pCollection.apply(ParDo.of(new FilterHeaderFunction(header)));
        PCollection<CustomerEntity> customerEntityPCollection
                = vals.apply(ParDo.of(new CustomerEntityMappingFunction()));



        PCollectionList<CustomerEntity> partitionList = customerEntityPCollection.apply(Partition.of(3, new CityBasedPartitionFunction()));
        PCollection<String> p0 = partitionList.get(0).apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity entity) -> entity.toString()));
        PCollection<String> p1 = partitionList.get(1).apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity entity) -> entity.toString()));
        PCollection<String> p2 = partitionList.get(2).apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity entity) -> entity.toString()));

        p0.apply(TextIO.write().to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/partition1.csv").withNumShards(1).withSuffix(".csv"));
        p1.apply(TextIO.write().to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/partition2.csv").withNumShards(1).withSuffix(".csv"));
        p2.apply(TextIO.write().to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/partition3.csv").withNumShards(1).withSuffix(".csv"));


        p.run();
    }
}

package com.anand.techservices;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.parquet.ParquetIO;

class ConvertCsvToGeneric extends SimpleFunction<String, GenericRecord> {
    @Override
    public GenericRecord apply(String input) {
        String[] arr = input.split(",");
        Schema schema = BeamCustomUtil.getSchema();
        GenericRecord genericRecord = new GenericData.Record(schema);

        genericRecord.put("SessionId", arr[0]);
        genericRecord.put("UserId", arr[1]);
        genericRecord.put("UserName", arr[2]);
        genericRecord.put("VideoId", arr[3]);
        genericRecord.put("Duration", Integer.parseInt(arr[4]));
        genericRecord.put("StartedTime", arr[5]);
        genericRecord.put("Sex", arr[6]);

        return genericRecord;
    }
}

public class ParquetIOWriteExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        Schema schema = BeamCustomUtil.getSchema();

        PCollection<String> pCollection = p.apply(TextIO.read().from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/user.csv"));

        final String header = "SessionId,UserId,UserName,VideoId,Duration,StartTime,Sex";
        PCollection<String> vals = pCollection.apply(ParDo.of(new FilterHeaderFunction(header)));

        vals.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                System.out.println(ctx.element());
            }
        }));

        PCollection<GenericRecord> pOutput = vals
                .apply(MapElements.via(new ConvertCsvToGeneric()))
                .setCoder(AvroCoder.of(GenericRecord.class, schema));

        pOutput.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema))
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/parquet_output")
                .withNumShards(1)
                .withSuffix(".parquet"));
        p.run();

    }
}

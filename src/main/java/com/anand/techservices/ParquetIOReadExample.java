package com.anand.techservices;

import com.anand.techservices.domain.VideoStoreUser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


class PrintElementFunction extends DoFn<GenericRecord, VideoStoreUser> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        GenericRecord record = ctx.element();

        String sessionId =  record.get("SessionId").toString();
        String userId =  record.get("UserId").toString();
        String userName =  record.get("UserName").toString();
        String videoId =  record.get("VideoId").toString();
        String duration =  record.get("Duration").toString();
        String startedTime =  record.get("StartedTime").toString();
        String sex =  record.get("Sex").toString();

        ctx.output(new VideoStoreUser(sessionId, userId, userName, videoId, duration, startedTime, sex));

    }
}

public class ParquetIOReadExample {
    public static void main(String[] args) {

        Pipeline p = Pipeline.create();
        Schema schema = BeamCustomUtil.getSchema();

        PCollection<GenericRecord> pCollection = p.apply(ParquetIO.read(schema)
                .from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/parquet_output/output-00000-of-00001.parquet"));

        PCollection<VideoStoreUser> videoStoreUserPCollection = pCollection.apply(ParDo.of(new PrintElementFunction()));
        PCollection<String> videoStoreUserStringPCollection = videoStoreUserPCollection.apply(MapElements.into(TypeDescriptors.strings()).via(
                (VideoStoreUser videoStoreUser) -> videoStoreUser.toString()
        ));

        videoStoreUserStringPCollection.apply(TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/video_store_user_output")
                .withNumShards(1)
                .withSuffix(".csv")
        );
        p.run();


    }
}

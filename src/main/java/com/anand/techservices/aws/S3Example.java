package com.anand.techservices.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class S3Example {
    public static void main(String[] args) {
        // AKIAZ3GGBWFFONBKPTPO,BC92QgCVUYz34T2ifGN93wudiJ4NlRIWoua/An62
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        AWSCredentials credentials = new BasicAWSCredentials(options.getAWSAccessKey(), options.getAWSSecretKey());
        options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(credentials));

        PCollection<String> inputCollection = p.apply(TextIO.read().from("s3://aa-beam-udemy-training/user_order.csv"));
        inputCollection.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                System.out.println(ctx.element());
            }
        }));


        p.run();

    }
}

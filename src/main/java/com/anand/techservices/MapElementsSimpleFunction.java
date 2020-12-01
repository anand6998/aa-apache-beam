package com.anand.techservices;

import com.anand.techservices.domain.VideoStoreUser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;


class UserMappingFunction extends SimpleFunction<String, VideoStoreUser> {
    @Override
    public VideoStoreUser apply(String input) {
        String[] arr = input.split(",");

        String sessionId = arr[0];
        String userId = arr[1];
        String userName = arr[2];
        String videoId = arr[3];
        String duration = arr[4];
        String startTime = arr[5];
        String sex = arr[6];
        //.equals("1") ? "M" : "F";
        String strSex = sex;
        switch (sex) {
            case "1":
                strSex = "M";
                break;
            case "2":
                strSex = "F";
                break;
            default:

        }

        final VideoStoreUser videoStoreUser = new VideoStoreUser(
                sessionId,
                userId,
                userName,
                videoId,
                duration,
                startTime,
                strSex
        );

        return videoStoreUser;
    }
}

public class MapElementsSimpleFunction {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        final ClassPathXmlApplicationContext ctx =
                new ClassPathXmlApplicationContext("applicationContext.xml");

        Resource resource = ctx.getResource("classpath:/user.csv");
        Pipeline p = Pipeline.create();

        PCollection<String> pUserList = p.apply(TextIO.read().from(resource.getFile().getAbsolutePath()));
        PCollection<VideoStoreUser> pOutput = pUserList.apply(MapElements.via(new UserMappingFunction()));
        PCollection<String> pStrUserOutput = pOutput.apply(MapElements.into(TypeDescriptors.strings())
                .via((VideoStoreUser user) -> user.toString()));

        pStrUserOutput.apply(
                TextIO.write()
                .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/video_store_user_out.csv")
                .withNumShards(1).withSuffix(".csv")
        );

        p.run();

    }
}

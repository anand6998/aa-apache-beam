package com.anand.techservices;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;



public class BeamCustomUtil {
    public static Schema getSchema() {
        String SCHEMA_STRING =
                "{\"namespace\": \"training.section6\",\n"
                        + " \"type\": \"record\",\n"
                        + " \"name\": \"ParquetExample\",\n"
                        + " \"fields\": [\n"
                        + "     {\"name\": \"SessionId\", \"type\": \"string\"},\n"
                        + "     {\"name\": \"UserId\", \"type\": \"string\"},\n"
                        + "     {\"name\": \"UserName\", \"type\": \"string\"},\n"
                        + "     {\"name\": \"VideoId\", \"type\": \"string\"},\n"
                        + "     {\"name\": \"Duration\", \"type\": \"int\"},\n"
                        + "     {\"name\": \"StartedTime\", \"type\": \"string\"},\n"
                        + "     {\"name\": \"Sex\", \"type\": \"string\"}\n"
                        + " ]\n"
                        + "}";
        Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
        return SCHEMA;

    }
}

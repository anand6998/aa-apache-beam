package com.anand;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WithKeysTest {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<String> words = p.apply( Create.of("Hello", "World", "Beam", "is", "fun"));
        PCollection<KV<Integer, String>> lengthAndWord =
                words.apply(WithKeys.of(new SerializableFunction<String, Integer>() {
                    @Override
                    public Integer apply(String s) {
                        return s.length();
                    }
                }));

        lengthAndWord.apply(ParDo.of(new DoFn<KV<Integer, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                KV<Integer, String> element = ctx.element();
                System.out.println(element.getKey() + ", " + element.getValue());
            }
        }));

        p.run();
    }
}

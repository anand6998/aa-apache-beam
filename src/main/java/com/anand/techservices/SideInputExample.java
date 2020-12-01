package com.anand.techservices;

import com.anand.techservices.domain.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Map;
import java.util.Objects;

public class SideInputExample {
    public static void main(String[] args) throws Exception {
        Pipeline p = Pipeline.create();

        PCollection<KV<String, String>> pReturn = p.apply(TextIO.read()
                .from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/side_inputs/return.csv")
        ).apply(ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                String[] arr = ctx.element().split(",");
                ctx.output(KV.of(arr[0], arr[1]));
            }
        }));

        PCollectionView<Map<String, String>> pMap = pReturn.apply(View.asMap());


        PCollection<String> pCustList = p.apply(TextIO.read()
                .from("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/side_inputs/cust_order.csv"));

        PCollection<CustomerEntity> customerEntityList = pCustList.apply(ParDo.of(new DoFn<String, CustomerEntity>() {
            @ProcessElement
            public void process(ProcessContext ctx) {
                Map<String, String> psideInputView = ctx.sideInput(pMap);

                String arr[] = ctx.element().split(",");
                String custName = psideInputView.get(arr[0]);

                if (Objects.isNull(custName)) {
                    CustomerEntity customerEntity = new CustomerEntity(Integer.valueOf(arr[0]), arr[1], arr[2], arr[3]);
                    ctx.output(customerEntity);
                }
            }
        }).withSideInputs(pMap));


        PCollection<String> custList = customerEntityList.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity entity) -> entity.toString()));
        custList.apply(TextIO.write().to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/side_input_example.csv").withNumShards(1).withSuffix(".csv"));

        p.run();
    }
}

package com.anand.techservices;

import lombok.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
class Product implements Serializable {
    String name;
    String city;
    String currency;


    public static Product of(String name, String city, String currency) {
        Product product = new Product();
        product.city = city;
        product.name = name;
        product.currency = currency;

        return product;
    }
}


public class JDBCIOExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();


        final String productName = "iphone";

        String redisHostUrl = "redis://redis_vm:6379";
        RedisConnectionConfiguration
                redisConnectionConfiguration = RedisConnectionConfiguration.create()
                .withHost("redis_vm")
                .withPort(6379);

        Jedis jedis =
                redisConnectionConfiguration.connect();



        PCollection<Product> productPCollection = p.apply(JdbcIO.<Product>read()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver",
                                "jdbc:postgresql://localhost:5432/products?useSSL=false"
                        )
                                .withUsername("postgres")
                                .withPassword("postgres")

                )
                .withQuery("SELECT name, city, currency from products.dbo.product_info WHERE name = ?")
                .withCoder(SerializableCoder.of(Product.class))
                .withStatementPreparator(new JdbcIO.StatementPreparator() {
                    @Override
                    public void setParameters(PreparedStatement preparedStatement) throws Exception {
                        preparedStatement.setString(1, productName);
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<Product>() {
                    @Override
                    public Product mapRow(ResultSet resultSet) throws Exception {
                        String name = resultSet.getString(1);
                        String city = resultSet.getString(2);
                        String currency = resultSet.getString(3);

                        return new Product(name, city, currency);
                    }
                })
        );

        PCollection<String> productStringPCollection = productPCollection.apply(ParDo.of(new DoFn<Product, String>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                Product product = ctx.element();
                ctx.output(product.toString());
            }

        }));

        productStringPCollection.apply(
                TextIO.write()
                        .to("/Users/anand/Documents/Amit/projects/apache-beam-training-2/src/main/resources/output/product_output")
                        .withSuffix(".csv")
                        .withNumShards(1)
        );

        p.run();

    }
}

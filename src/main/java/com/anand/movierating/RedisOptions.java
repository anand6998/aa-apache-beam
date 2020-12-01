package com.anand.movierating;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.redisson.api.RedissonClient;

public interface RedisOptions extends PipelineOptions {
    ValueProvider<String> getRedisHost();
    void setRedisHost(ValueProvider<String> value);

    ValueProvider<Integer> getRedisPort();
    void setRedisPort(ValueProvider<Integer> value);


    @Default.InstanceFactory(RedisClientFactory.class)
    RedissonClient getRedissonClient();
    void setRedissonClient(RedissonClient value);

}

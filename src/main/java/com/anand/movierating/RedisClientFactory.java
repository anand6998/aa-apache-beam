package com.anand.movierating;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedisClientFactory implements DefaultValueFactory<RedissonClient> {
    @Override
    public RedissonClient create(PipelineOptions options) {
        RedisOptions redisOptions = options.as(RedisOptions.class);
        Config config = new Config();

        config.useSingleServer()
                .setAddress("redis://" + redisOptions.getRedisHost() + ":" + redisOptions.getRedisPort());
        return Redisson.create(config);
    }
}

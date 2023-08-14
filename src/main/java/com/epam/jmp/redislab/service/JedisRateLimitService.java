package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class JedisRateLimitService implements RateLimitService{


    private final Map<String, RateLimitRule> rules;
    private JedisCluster jedisCluster;

    public JedisRateLimitService(@Autowired Set<RateLimitRule> rateLimitRules, @Autowired JedisCluster jedisCluster) {
        this.rules = getRules(rateLimitRules);
        this.jedisCluster = jedisCluster;
    }

    private Map<String, RateLimitRule> getRules(Set<RateLimitRule> rateLimitRules) {
        return rateLimitRules.stream()
                .collect(Collectors.toMap(
                        rateLimitRule -> getKey(rateLimitRule.getAccountId(), rateLimitRule.getClientIp(), rateLimitRule.getRequestType()),
                        Function.identity())
                );
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {

        var descriptorKeys = requestDescriptors.stream()
                .map(descriptor -> {
                    var sourceKey = getKey(descriptor.getAccountId(), descriptor.getClientIp(), descriptor.getRequestType());
                    if (Objects.nonNull(rules.get(sourceKey))){
                        return sourceKey;
                    }
                    var allKey = getKey(Optional.of(""), Optional.of(""), descriptor.getRequestType());
                    if (Objects.nonNull(rules.get(allKey))){
                        return allKey;
                    }
                    var allAccKey = getKey(Optional.of(""), descriptor.getClientIp(), descriptor.getRequestType());
                    if (Objects.nonNull(rules.get(allAccKey))){
                        return allAccKey;
                    }
                    var allCliIpKey = getKey(descriptor.getAccountId(), Optional.of(""), descriptor.getRequestType());
                    if (Objects.nonNull(rules.get(allCliIpKey))){
                        return allCliIpKey;
                    }
                    return "";

                })
                .toList();

        boolean shouldNotLimit = descriptorKeys.stream().allMatch(rules::containsKey);

        if (shouldNotLimit){
            return descriptorKeys.stream()
                    .allMatch(this::processLimit);
        }
        return true;
    }

    private boolean processLimit(String descriptorKey) {
        var limitRule = rules.get(descriptorKey);
        var customerLimitData = jedisCluster.get(descriptorKey);

        if (Objects.isNull(customerLimitData)) {
            jedisCluster.setex(descriptorKey, limitRule.getTimeInterval().getSeconds(), "1");
            return false;
        }
        int numberOfRequests = Integer.parseInt(customerLimitData);

        if (numberOfRequests >= limitRule.getAllowedNumberOfRequests()) {
            return true;
        }
        jedisCluster.incr(descriptorKey);
        return false;
    }

    @SafeVarargs
    private String getKey(Optional<String>... values) {
        return Arrays.stream(values)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(val -> Objects.equals(val, "") ?":all:" : val)
                .reduce("", String::concat);
    }
}

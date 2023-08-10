package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class JedisRateLimitService implements RateLimitService{
    private final String SEPARATOR = ":";
    private final Map<String, RateLimitRule> rules;

    private final JedisCluster jedisCluster;

    public JedisRateLimitService(Set<RateLimitRule> rateLimitRules, JedisCluster jedisCluster) {
        this.rules = rateLimitRules.stream()
                .collect(Collectors.toMap(this::getKey, Function.identity()));

        this.jedisCluster = jedisCluster;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {

        return false;
    }

    public String getKey(RateLimitRule rule) {
        StringBuilder key = new StringBuilder();

        rule.getAccountId().ifPresent(id -> key.append(id).append(SEPARATOR));
        rule.getClientIp().ifPresent(ip -> key.append(ip).append(SEPARATOR));
        rule.getRequestType().ifPresent(key::append);

        return key.toString();
    }
}

package com.sparta.gateway.server.application;

import com.sparta.gateway.server.infrastructure.exception.GatewayErrorCode;
import com.sparta.gateway.server.infrastructure.exception.GatewayException;
import java.time.Instant;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class UserQueueRepository {

  private static final String USER_QUEUE_WAIT_KEY = "users:queue:wait";
  private static final String USER_QUEUE_ACTIVE_KEY = "users:queue:active";
  private static final long INACTIVITY_THRESHOLD = 300;

  private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

  private long getCurrentTime() {
    return Instant.now().getEpochSecond();
  }

  public Mono<Long> findActiveUserRank(String userId) {
    return reactiveRedisTemplate.opsForZSet().rank(USER_QUEUE_ACTIVE_KEY, userId);
  }

  public Mono<Long> findWaitingUserRank(String userId) {
    return reactiveRedisTemplate.opsForZSet().rank(USER_QUEUE_WAIT_KEY, userId);
  }

  public Mono<Boolean> addToActiveQueue(String userId) {
    return reactiveRedisTemplate.opsForZSet().add(USER_QUEUE_ACTIVE_KEY, userId, getUniqueScore());
  }

  public Mono<Boolean> addToWaitingQueue(String userId) {
    return reactiveRedisTemplate.opsForZSet().add(USER_QUEUE_WAIT_KEY, userId, getUniqueScore());
  }

  public Mono<Boolean> isUserInActiveQueue(String userId) {
    return reactiveRedisTemplate.opsForZSet()
        .rank(USER_QUEUE_ACTIVE_KEY, userId)
        .map(Objects::nonNull);
  }

  public Mono<Long> countActiveUsers() {
    return reactiveRedisTemplate.opsForZSet().size(USER_QUEUE_ACTIVE_KEY);
  }

  public Mono<Void> removeInactiveUsers(long maxActiveUsers) {
    long currentTime = getCurrentTime();
    return reactiveRedisTemplate.opsForZSet()
        .rangeWithScores(USER_QUEUE_ACTIVE_KEY, Range.closed(0L, -1L))
        .filter(userWithScore -> currentTime - userWithScore.getScore() > INACTIVITY_THRESHOLD)
        .flatMap(userWithScore ->
            reactiveRedisTemplate.opsForZSet()
                .remove(USER_QUEUE_ACTIVE_KEY, userWithScore.getValue())
        )
        .then();
  }

  public Mono<Long> moveWaitingUsersToActive(long maxActiveUsers) {
    return reactiveRedisTemplate.opsForZSet()
        .size(USER_QUEUE_ACTIVE_KEY)
        .flatMap(activeUsers -> {
          long slotsAvailable = maxActiveUsers - activeUsers;
          return slotsAvailable <= 0
              ? Mono.just(0L)
              : moveUsersToActiveQueue(slotsAvailable);
        });
  }

  private Mono<Long> moveUsersToActiveQueue(long count) {
    return reactiveRedisTemplate.opsForZSet()
        .popMin(USER_QUEUE_WAIT_KEY, count)
        .flatMap(user -> {
          String userId = Objects.requireNonNull(user.getValue());
          return reactiveRedisTemplate.opsForZSet()
              .add(USER_QUEUE_ACTIVE_KEY, userId, getCurrentTime())
              .filter(Boolean::booleanValue)
              .switchIfEmpty(Mono.error(new GatewayException(GatewayErrorCode.TOO_MANY_REQUESTS)))
              .thenReturn(1L);
        })
        .count();
  }


  public Mono<Boolean> updateUserActivityTime(String userId) {
    return reactiveRedisTemplate.opsForZSet().add(USER_QUEUE_ACTIVE_KEY, userId, getUniqueScore());
  }

  private double getUniqueScore() {
    long millis = System.currentTimeMillis();
    double randomFraction = Math.random();
    return millis + randomFraction;
  }

}

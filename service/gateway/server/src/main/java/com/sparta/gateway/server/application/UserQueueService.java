package com.sparta.gateway.server.application;

import com.sparta.gateway.server.application.dto.RegisterUserResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserQueueService {

  private final UserQueueRepository userQueueRepository;

  @Value("${MAX_ACTIVE_USERS}")
  private long maxActiveUsers;

  public Mono<RegisterUserResponse> registerUser(String userId) {
    return userQueueRepository.findActiveUserRank(userId)
        .flatMap(rank -> Mono.just(new RegisterUserResponse(rank + 1)))
        .switchIfEmpty(handleNewUserRegistration(userId));
  }

  private Mono<RegisterUserResponse> handleNewUserRegistration(String userId) {
    return userQueueRepository.findWaitingUserRank(userId)
        .flatMap(waitRank -> Mono.just(new RegisterUserResponse(waitRank + 1)))
        .switchIfEmpty(processUserEntryToQueue(userId));
  }

  private Mono<RegisterUserResponse> processUserEntryToQueue(String userId) {
    return userQueueRepository.countActiveUsers()
        .flatMap(activeUsers -> activeUsers < maxActiveUsers
            ? addUserToActiveQueue(userId, activeUsers)
            : addUserToWaitingQueue(userId));
  }

  private Mono<RegisterUserResponse> addUserToActiveQueue(String userId, long activeUsers) {
    return userQueueRepository.addToActiveQueue(userId)
        .thenReturn(new RegisterUserResponse(activeUsers + 1));
  }

  private Mono<RegisterUserResponse> addUserToWaitingQueue(String userId) {
    return userQueueRepository.addToWaitingQueue(userId)
        .flatMap(added -> userQueueRepository.findWaitingUserRank(userId))
        .map(rank -> new RegisterUserResponse(rank + 1));
  }

  public Mono<Boolean> isAllowed(String userId) {
    return userQueueRepository.isUserInActiveQueue(userId)
        .flatMap(isAllowed -> isAllowed
            ? userQueueRepository.updateUserActivityTime(userId).thenReturn(true)
            : Mono.just(false));
  }

  public Mono<Long> getRank(String userId) {
    return userQueueRepository.findWaitingUserRank(userId)
        .map(rank -> rank >= 0 ? rank + 1 : rank);
  }

  public void scheduleAllowUser() {
    userQueueRepository.removeInactiveUsers(maxActiveUsers)
        .then(userQueueRepository.moveWaitingUsersToActive(maxActiveUsers))
        .subscribe(
            count -> log.info("Moved {} users to active queue", count),
            error -> log.error("Error in user queue scheduling", error)
        );
  }

}

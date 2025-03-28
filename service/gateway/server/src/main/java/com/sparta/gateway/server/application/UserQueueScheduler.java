package com.sparta.gateway.server.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserQueueScheduler {

  private final UserQueueService userQueueService;

  @Scheduled(fixedRate = 10000, initialDelay = 500)
  public void scheduleAllowUser() {
    userQueueService.scheduleAllowUser();
  }

}

package com.sparta.gateway.server.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
@Slf4j
@RequiredArgsConstructor
public class QueueRequestProcessor {

  private final UserQueueService userQueueService;

  public Mono<Void> processRequest(ServerWebExchange exchange, GatewayFilterChain chain,
      String userId) {
    return userQueueService.isAllowed(userId)
        .flatMap(allowed -> {
          if (allowed) {
            return chain.filter(exchange);
          }
          return registerAndHandleUserInQueue(exchange, chain, userId);
        });
  }

  private Mono<Void> registerAndHandleUserInQueue(ServerWebExchange exchange,
      GatewayFilterChain chain, String userId) {
    return userQueueService.registerUser(userId)
        .flatMap(response -> {
          if (response.getRank() == 0) {
            return chain.filter(exchange);
          }

          var responseHeaders = exchange.getResponse().getHeaders();
          responseHeaders.add("X-Queue-Rank", String.valueOf(response.getRank()));
          exchange.getResponse().setStatusCode(HttpStatus.OK);

          return exchange.getResponse().setComplete();
        });
  }

}

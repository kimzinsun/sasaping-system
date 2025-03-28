package com.sparta.gateway.server.infrastructure.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparta.gateway.server.application.QueueRequestProcessor;
import com.sparta.gateway.server.application.UserExtractor;
import com.sparta.gateway.server.application.UserQueueService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class GlobalQueueFilter implements GlobalFilter, Ordered {

  private final UserExtractor userExtractor;
  private final QueueRequestProcessor queueRequestProcessor;

  public GlobalQueueFilter(UserQueueService userQueueService, ObjectMapper objectMapper,
      UserExtractor userExtractor, QueueRequestProcessor queueRequestProcessor) {

    this.userExtractor = userExtractor;
    this.queueRequestProcessor = queueRequestProcessor;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

    String path = exchange.getRequest().getURI().getPath();

    if (isPublicPath(path)) {
      return chain.filter(exchange);
    }

    return userExtractor.extractUserId(exchange)
        .flatMap(userId -> queueRequestProcessor.processRequest(exchange, chain, userId));
  }

  private boolean isPublicPath(String path) {
    return path.startsWith("/api/auth/")
        || path.startsWith("/api/users/sign-up")
        || path.startsWith("/api/search")
        || path.startsWith("/api/products/search")
        || path.startsWith("/api/preorder/search")
        || path.startsWith("/api/categories/search");
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }


}

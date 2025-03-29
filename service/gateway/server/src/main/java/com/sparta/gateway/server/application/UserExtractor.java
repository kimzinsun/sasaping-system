package com.sparta.gateway.server.application;

import static com.sparta.common.domain.jwt.JwtGlobalConstant.X_USER_CLAIMS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparta.auth.auth_dto.jwt.JwtClaim;
import com.sparta.gateway.server.infrastructure.exception.GatewayErrorCode;
import com.sparta.gateway.server.infrastructure.exception.GatewayException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class UserExtractor {

  private final ObjectMapper objectMapper;

  public UserExtractor(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public Mono<String> extractUserId(ServerWebExchange exchange) {
    String encodedClaims = exchange.getRequest().getHeaders().getFirst(X_USER_CLAIMS);
    if (encodedClaims == null) {
      return Mono.error(new GatewayException(GatewayErrorCode.UNAUTHORIZED));
    }

    String decodedClaims = URLDecoder.decode(encodedClaims, StandardCharsets.UTF_8);
    try {
      JwtClaim claims = objectMapper.readValue(decodedClaims, JwtClaim.class);
      return Mono.just(claims.getUserId().toString());
    } catch (JsonProcessingException e) {
      return Mono.error(new GatewayException(GatewayErrorCode.BAD_REQUEST));
    }
  }

}

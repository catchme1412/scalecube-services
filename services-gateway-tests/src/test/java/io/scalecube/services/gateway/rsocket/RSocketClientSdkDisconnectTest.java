package io.scalecube.services.gateway.rsocket;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;

import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientSettings;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import io.scalecube.services.gateway.clientsdk.exceptions.ConnectionClosedException;
import io.scalecube.services.gateway.clientsdk.rsocket.RSocketClientCodec;
import io.scalecube.services.gateway.clientsdk.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

class RSocketClientSdkDisconnectTest {

  private static final String GATEWAY_ALIAS_NAME = "rsws";
  private static final GatewayConfig gatewayConfig =
      GatewayConfig.builder(GATEWAY_ALIAS_NAME, RSocketGateway.class).build();
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(3);

  private static final String JOHN = "John";

  private LoopResources clientLoopResources;
  private Microservices seed;

  private Client rsocketClient;

  @BeforeEach
  void startClient() {
    seed =
        Microservices.builder()
            .services(new GreetingServiceImpl())
            .gateway(gatewayConfig)
            .discovery(ScalecubeServiceDiscovery::new)
            .startAwait();

    clientLoopResources = LoopResources.create("eventLoop");

    int gatewayPort =
        seed.gatewayAddress(GATEWAY_ALIAS_NAME, gatewayConfig.gatewayClass()).getPort();
    ClientSettings settings = ClientSettings.builder().port(gatewayPort).build();

    ClientCodec clientCodec =
        new RSocketClientCodec(
            HeadersCodec.getInstance(settings.contentType()),
            DataCodec.getInstance(settings.contentType()));

    ClientTransport clientTransport =
        new RSocketClientTransport(settings, clientCodec, clientLoopResources);

    rsocketClient = new Client(clientTransport, clientCodec);
  }

  @AfterEach
  void stopClient() {
    if (rsocketClient != null) {
      rsocketClient.close().block(SHUTDOWN_TIMEOUT);
    }
    if (clientLoopResources != null) {
      clientLoopResources.disposeLater().block(SHUTDOWN_TIMEOUT);
    }
    if (seed != null) {
      try {
        seed.shutdown().block(SHUTDOWN_TIMEOUT);
      } catch (Exception ignore) {
        // no-op
      }
    }
  }

  @Test
  void testServerDisconnection() {
    Duration shutdownAt = Duration.ofSeconds(1);

    StepVerifier.create(
            rsocketClient
                .forService(GreetingService.class)
                .many(JOHN)
                .doOnSubscribe(
                    subscription ->
                        Mono.delay(shutdownAt)
                            .doOnSuccess(ignore -> seed.shutdown().subscribe())
                            .subscribe()))
        .thenConsumeWhile(
            response -> {
              assertThat(response, startsWith("Greeting ("));
              assertThat(response, endsWith(") to: " + JOHN));
              return true;
            })
        .expectError(ConnectionClosedException.class)
        .verify(shutdownAt.plus(SHUTDOWN_TIMEOUT));
  }
}

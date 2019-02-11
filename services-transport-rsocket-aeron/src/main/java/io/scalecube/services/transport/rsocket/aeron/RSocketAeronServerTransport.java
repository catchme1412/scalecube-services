package io.scalecube.services.transport.rsocket.aeron;

import io.rsocket.Closeable;
import io.rsocket.RSocketFactory;
import io.rsocket.reactor.aeron.AeronServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.core.publisher.Mono;

/** RSocket Aeron server transport implementation. */
public class RSocketAeronServerTransport implements ServerTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketAeronServerTransport.class);

  private final ServiceMessageCodec codec;
  private final AeronResources aeronResources;

  private Closeable server; // calculated

  /**
   * Constructor for this server transport.
   *
   * @param codec message codec
   * @param aeronResources aeron server resources
   */
  public RSocketAeronServerTransport(ServiceMessageCodec codec, AeronResources aeronResources) {
    this.codec = codec;
    this.aeronResources = aeronResources;
  }

  @Override
  public Mono<InetSocketAddress> bind(int port, ServiceMethodRegistry methodRegistry) {
    return Mono.defer(
        () -> {
          InetSocketAddress bindAddress = new InetSocketAddress(port);
          AeronServer aeronServer =
              AeronServer.create(aeronResources).options("0.0.0.0", port, port + 1);
          return RSocketFactory.receive()
              .frameDecoder(
                  frame ->
                      ByteBufPayload.create(
                          frame.sliceData().retain(), frame.sliceMetadata().retain()))
              .acceptor(new RSocketAeronServiceAcceptor(codec, methodRegistry))
              .transport(() -> new AeronServerTransport(aeronServer))
              .start()
              .map(server -> this.server = server)
              .thenReturn(bindAddress);
        });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () ->
            Optional.ofNullable(server)
                .map(
                    server -> {
                      server.dispose();
                      return server
                          .onClose()
                          .doOnError(e -> LOGGER.warn("Failed to close server: " + e))
                          .onErrorResume(e -> Mono.empty());
                    })
                .orElse(Mono.empty()));
  }
}

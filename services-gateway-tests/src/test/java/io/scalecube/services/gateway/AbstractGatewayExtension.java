package io.scalecube.services.gateway;

import static io.scalecube.services.discovery.ClusterAddresses.toAddress;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientSettings;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGatewayExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGatewayExtension.class);

  private final Microservices gateway;
  private final Object serviceInstance;
  private final GatewayConfig gatewayConfig;

  private Client client;
  private InetSocketAddress gatewayAddress;
  private Microservices services;

  protected AbstractGatewayExtension(Object serviceInstance, GatewayConfig gatewayConfig) {
    this.gatewayConfig = gatewayConfig;
    this.serviceInstance = serviceInstance;

    gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .gateway(gatewayConfig)
            .startAwait();
  }

  @Override
  public final void afterAll(ExtensionContext context) {
    shutdownServices();
    shutdownGateway();
  }

  @Override
  public final void afterEach(ExtensionContext context) {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public final void beforeAll(ExtensionContext context) {
    gatewayAddress = gateway.gatewayAddress(gatewayAliasName(), gatewayConfig.gatewayClass());
    startServices();
  }

  @Override
  public final void beforeEach(ExtensionContext context) {
    // if services was shutdown in test need to start them again
    if (services == null) {
      startServices();
    }
    client = new Client(transport(), clientMessageCodec());
  }

  public Client client() {
    return client;
  }

  /** Start services. */
  public void startServices() {
    services =
        Microservices.builder()
            .discovery(this::serviceDiscovery)
            .services(serviceInstance)
            .startAwait();
    LOGGER.info("Started services {} on {}", services, services.serviceAddress());
  }

  private ServiceDiscovery serviceDiscovery(
      ServiceRegistry serviceRegistry, ServiceEndpoint serviceEndpoint) {
    return new ScalecubeServiceDiscovery(serviceRegistry, serviceEndpoint)
        .options(opts -> opts.seedMembers(toAddress(gateway.discovery().address())));
  }

  /** Shutdown services. */
  public void shutdownServices() {
    if (services != null) {
      try {
        services.shutdown().block();
      } catch (Throwable ignore) {
        // ignore
      }
      LOGGER.info("Shutdown services {}", services);

      // if this method is called in particular test need to indicate that services are stopped to
      // start them again before another test
      services = null;
    }
  }

  private void shutdownGateway() {
    if (gateway != null) {
      try {
        gateway.shutdown().block();
      } catch (Throwable ignore) {
        // ignore
      }
      LOGGER.info("Shutdown gateway {}", gateway);
    }
  }

  protected final ClientSettings clientSettings() {
    return ClientSettings.builder()
        .host(gatewayAddress.getHostName())
        .port(gatewayAddress.getPort())
        .build();
  }

  protected abstract ClientTransport transport();

  protected abstract ClientCodec clientMessageCodec();

  protected abstract String gatewayAliasName();
}

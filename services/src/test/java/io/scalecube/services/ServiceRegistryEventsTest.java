package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.sut.GreetingServiceImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class ServiceRegistryEventsTest {

  @Test
  public void test_added_removed_registration_events() {

    List<ServiceDiscoveryEvent> events = new ArrayList<>();

    Microservices seed = new Microservices().startAwait();

    seed.discovery().listen().subscribe(events::add);

    Microservices ms1 = getMs(seed);

    Microservices ms2 = getMs(seed);

    Mono.when(ms1.shutdown(), ms2.shutdown()).block(Duration.ofSeconds(6));

    assertEquals(4, events.size());

    seed.shutdown().block(Duration.ofSeconds(6));
  }

  private Microservices getMs(Microservices seed) {
    return new Microservices()
        .discovery(options -> options.seeds(seed.discovery().address()))
        .services(new GreetingServiceImpl())
        .startAwait();
  }
}

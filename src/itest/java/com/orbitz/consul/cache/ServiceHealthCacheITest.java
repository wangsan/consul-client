package com.orbitz.consul.cache;

import com.google.common.collect.ImmutableMap;
import com.orbitz.consul.BaseIntegrationTest;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.Synchroniser;
import com.orbitz.consul.model.health.ServiceHealth;
import org.junit.Test;

import java.sql.Time;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ServiceHealthCacheITest extends BaseIntegrationTest {

    private static final List<String> NO_TAGS = Collections.emptyList();
    private static final Map<String, String> NO_META = Collections.emptyMap();

    @Test
    public void nodeCacheServicePassingTest() throws Exception {
        HealthClient healthClient = client.healthClient();
        String serviceName = UUID.randomUUID().toString();
        String serviceId = UUID.randomUUID().toString();

        client.agentClient().register(8080, 20L, serviceName, serviceId, NO_TAGS, NO_META);
        client.agentClient().pass(serviceId);
        Synchroniser.pause(Duration.ofMillis(100));

        try (ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, serviceName)) {
            svHealth.start();
            svHealth.awaitInitialized(3, TimeUnit.SECONDS);

            ServiceHealthKey serviceKey = getServiceHealthKeyFromCache(svHealth, serviceId, 8080)
                    .orElseThrow(() -> new RuntimeException("Cannot find service key from serviceHealthCache"));

            ServiceHealth health = svHealth.getMap().get(serviceKey);
            assertNotNull(health);
            assertEquals(serviceId, health.getService().getId());

            client.agentClient().fail(serviceId);
            Synchroniser.pause(Duration.ofMillis(100));
            health = svHealth.getMap().get(serviceKey);
            assertNull(health);
        }
    }

    @Test
    public void testServicesAreUniqueByID() throws Exception {
        HealthClient healthClient = client.healthClient();
        String serviceName = UUID.randomUUID().toString();
        String serviceId = UUID.randomUUID().toString();
        String serviceId2 = UUID.randomUUID().toString();

        client.agentClient().register(8080, 20L, serviceName, serviceId, NO_TAGS, NO_META);
        client.agentClient().pass(serviceId);

        client.agentClient().register(8080, 20L, serviceName, serviceId2, NO_TAGS, NO_META);
        client.agentClient().pass(serviceId2);

        try (ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, serviceName)) {
            svHealth.start();
            svHealth.awaitInitialized(3, TimeUnit.SECONDS);

            ServiceHealthKey serviceKey1 = getServiceHealthKeyFromCache(svHealth, serviceId, 8080)
                    .orElseThrow(() -> new RuntimeException("Cannot find service key 1 from serviceHealthCache"));

            ServiceHealthKey serviceKey2 = getServiceHealthKeyFromCache(svHealth, serviceId2, 8080)
                    .orElseThrow(() -> new RuntimeException("Cannot find service key 2 from serviceHealthCache"));

            ImmutableMap<ServiceHealthKey, ServiceHealth> healthMap = svHealth.getMap();
            assertEquals(healthMap.size(), 2);
            ServiceHealth health = healthMap.get(serviceKey1);
            ServiceHealth health2 = healthMap.get(serviceKey2);

            assertEquals(serviceId, health.getService().getId());
            assertEquals(serviceId2, health2.getService().getId());
        }
    }

    private static Optional<ServiceHealthKey> getServiceHealthKeyFromCache(ServiceHealthCache cache, String serviceId, int port) {
        return cache.getMap().keySet()
                .stream()
                .filter(key -> serviceId.equals(key.getServiceId()) && (port == key.getPort()))
                .findFirst();
    }

    @Test
    public void shouldNotifyListener() throws Exception {
        String serviceName = UUID.randomUUID().toString();
        String serviceId = UUID.randomUUID().toString();

        client.agentClient().register(8080, 20L, serviceName, serviceId, NO_TAGS, NO_META);
        client.agentClient().pass(serviceId);

        ServiceHealthCache svHealth = ServiceHealthCache.newCache(client.healthClient(), serviceName);

        final List<Map<ServiceHealthKey, ServiceHealth>> events = new ArrayList<>();
        svHealth.addListener(events::add);

        svHealth.start();
        svHealth.awaitInitialized(1000, TimeUnit.MILLISECONDS);

        Synchroniser.pause(Duration.ofMillis(200));
        client.agentClient().deregister(serviceId);
        Synchroniser.pause(Duration.ofMillis(200));

        assertEquals(2, events.size());
        Map<ServiceHealthKey, ServiceHealth> event0 = events.get(0);

        assertEquals(1, event0.size());
        for (Map.Entry<ServiceHealthKey, ServiceHealth> kv : event0.entrySet()) {
            assertEquals(kv.getKey().getServiceId(), serviceId);
        }

        Map<ServiceHealthKey, ServiceHealth> event1 = events.get(1);
        assertEquals(0, event1.size());
        svHealth.stop();
    }

    @Test
    public void shouldNotifyListener2() throws Exception {
        String serviceName = UUID.randomUUID().toString();
        String serviceId_1 = "test1";
        String serviceId_2 = "test2";
        String serviceId_3 = "test3";


        client.agentClient().register(8080, 20L, serviceName, serviceId_1, NO_TAGS, NO_META);
        client.agentClient().register(8081, 20L, serviceName, serviceId_2, NO_TAGS, NO_META);


        client.agentClient().pass(serviceId_1);

        ServiceHealthCache svHealth = ServiceHealthCache.newCache(client.healthClient(), serviceName);

        svHealth.addListener(kv -> {
            System.out.println(kv.size());
            kv.forEach((k, v) -> System.err.println(k));
        });


        svHealth.start();
        svHealth.awaitInitialized(1000, TimeUnit.MILLISECONDS);

        System.err.println("add service 3");
        client.agentClient().register(8082, 20L, serviceName, serviceId_3, NO_TAGS, NO_META);

        Synchroniser.pause(Duration.ofMillis(2000));
        System.err.println("pass service 3");
        client.agentClient().pass(serviceId_3);


        Synchroniser.pause(Duration.ofMillis(2000));
        System.err.println("pass service 3 2");
        client.agentClient().pass(serviceId_3);


        Synchroniser.pause(Duration.ofMillis(2000));
        System.err.println("fail service 2");
        client.agentClient().fail(serviceId_2);

        Synchroniser.pause(Duration.ofMillis(2000));
        System.err.println("pass service 2");
        client.agentClient().pass(serviceId_2);


        Synchroniser.pause(Duration.ofMillis(2000));
        System.err.println("fail service 2");
        client.agentClient().fail(serviceId_2);

        Synchroniser.pause(Duration.ofMillis(2000));
        svHealth.stop();
    }


    @Test
    public void shouldNotifyLateListenersIfNoService() throws Exception {
        String serviceName = UUID.randomUUID().toString();

        ServiceHealthCache svHealth = ServiceHealthCache.newCache(client.healthClient(), serviceName);

        final List<Map<ServiceHealthKey, ServiceHealth>> events = new ArrayList<>();
        svHealth.addListener(events::add);

        svHealth.start();
        svHealth.awaitInitialized(1000, TimeUnit.MILLISECONDS);

        assertEquals(1, events.size());
        Map<ServiceHealthKey, ServiceHealth> event0 = events.get(0);
        assertEquals(0, event0.size());
        svHealth.stop();
    }

    @Test
    public void shouldNotifyLateListenersRaceCondition() throws Exception {
        String serviceName = UUID.randomUUID().toString();

        final ServiceHealthCache svHealth = ServiceHealthCache.newCache(client.healthClient(), serviceName);

        final AtomicInteger eventCount = new AtomicInteger(0);
        svHealth.addListener(newValues -> {
            eventCount.incrementAndGet();
            Thread t = new Thread(() -> svHealth.addListener(newValues1 -> eventCount.incrementAndGet()));
            t.start();
            Synchroniser.pause(Duration.ofMillis(500));
        });

        svHealth.start();
        svHealth.awaitInitialized(1000, TimeUnit.MILLISECONDS);

        Synchroniser.pause(Duration.ofSeconds(1));
        assertEquals(2, eventCount.get());
        svHealth.stop();
    }
}

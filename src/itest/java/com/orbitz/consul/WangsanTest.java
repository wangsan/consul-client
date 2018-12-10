package com.orbitz.consul;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitz.consul.cache.KVCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.util.bookend.ConsulBookend;
import com.orbitz.consul.util.bookend.ConsulBookendContext;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class WangsanTest {

    Logger logger = LoggerFactory.getLogger(WangsanTest.class);
    Consul client; // connect on localhost


    @Before
    public void before() {
        client = Consul.builder()
                .withConsulBookend(new ConsulBookend() {
                    @Override
                    public void pre(String url, ConsulBookendContext context) {
                        System.out.println("req:" + url);
                    }

                    @Override
                    public void post(int code, ConsulBookendContext context) {
                        System.out.println("resp code:" + code);
                    }
                })
                .build();
    }

    @Test
    public void testRegister() throws NotRegisteredException {
        logger.info("test connect");


        AgentClient agentClient = client.agentClient();

        String serviceId = "1";
        Registration service = ImmutableRegistration.builder()
                .id(serviceId)
                .name("myService")
                .port(8080)
                .check(Registration.RegCheck.ttl(3L)) // registers with a TTL of 3 seconds
                .tags(Collections.singletonList("tag" + Math.random()))
                .meta(Collections.singletonMap("version", "1.0"))
                .build();

        agentClient.register(service);

// Check in with Consul (serviceId required only).
// Client will prepend "service:" for service level checks.
// Note that you need to continually check in before the TTL expires, otherwise your service's state will be marked as "critical".
        agentClient.pass(serviceId);
    }

    @Test
    public void testFindAvailable() {
        HealthClient healthClient = client.healthClient();

// Discover only "passing" nodes
        List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances("myService").getResponse();
        prettyPrint(nodes);


    }

    @Test
    public void testStoreKv() {
        KeyValueClient kvClient = client.keyValueClient();

        kvClient.putValue("foo", "bar");
        String value = kvClient.getValueAsString("foo").get(); // bar
        System.out.println(value);
    }

    @Test
    public void testSubscribeCache() {
        final KeyValueClient kvClient = client.keyValueClient();

        kvClient.putValue("foo", "bar");

        KVCache cache = KVCache.newCache(kvClient, "foo");
        cache.addListener(newValues -> {
            // Cache notifies all paths with "foo" the root path
            // If you want to watch only "foo" value, you must filter other paths
            Optional<Value> newValue = newValues.values().stream()
                    .filter(value -> value.getKey().equals("foo"))
                    .findAny();

            newValue.ifPresent(value -> {
                // Values are encoded in key/value store, decode it if needed
                Optional<String> decodedValue = newValue.get().getValueAsString();
                decodedValue.ifPresent(v -> System.out.println(String.format("Value is: %s", v))); //prints "bar"
            });
        });
        cache.start();

        kvClient.putValue("foo", "bar2");


        cache.stop();

    }

    @Test
    public void testSubscribeHealthyService() {
        HealthClient healthClient = client.healthClient();
        String serviceName = "myService";

        ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, serviceName);
        svHealth.addListener((Map<ServiceHealthKey, ServiceHealth> newValues) -> {
            // do something with updated server map
            newValues.forEach((k, v) -> System.out.println(k + "->" + v));
        });
        svHealth.start();
        svHealth.stop();

    }

    private void prettyPrint(Object obj) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

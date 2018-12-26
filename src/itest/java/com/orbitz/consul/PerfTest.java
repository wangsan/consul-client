package com.orbitz.consul;

import com.google.common.base.Stopwatch;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class PerfTest extends BaseIntegrationTest {


    @Test
    public void testRegisterPerf() throws InterruptedException {
        AgentClient agentClient = client.agentClient();

        int size=200;
        CountDownLatch latch=new CountDownLatch(size);
        Stopwatch stopwatch=Stopwatch.createStarted();
        IntStream.range(1000,1000+size).parallel().forEach(i->{
            Registration service = ImmutableRegistration.builder()
                    .id("test_id_"+i)
                    .name("myService")
                    .port(ThreadLocalRandom.current().nextInt(10000))
//                    .check(Registration.RegCheck.ttl(3L)) // registers with a TTL of 3 seconds
                    .tags(Collections.singletonList("tag" + Math.random()))
                    .meta(Collections.singletonMap("version", "1.0"))
                    .build();
            agentClient.register(service);
            latch.countDown();
        });

        latch.await();
        stopwatch.stop();
        System.out.println(stopwatch);

    }

    @Test
    public void testHealPerf(){
        HealthClient healthClient = client.healthClient();

        Stopwatch stopwatch=Stopwatch.createStarted();
        IntStream.range(0,1000).parallel().forEach(i->{
            ConsulResponse<List<ServiceHealth>> web = healthClient.getHealthyServiceInstances("web");
//            System.out.println(web.getResponse().size());
        });
        stopwatch.stop();
        System.out.println(stopwatch);

        Stopwatch stopwatch2=Stopwatch.createStarted();
        IntStream.range(0,1000).parallel().forEach(i->{
            ConsulResponse<List<ServiceHealth>> web = healthClient.getHealthyServiceInstances("myService");
//            System.out.println(web.getResponse().size());
        });
        stopwatch2.stop();
        System.out.println(stopwatch2);

        Stopwatch stopwatch3=Stopwatch.createStarted();
        IntStream.range(0,1000).parallel().forEach(i->{
            ConsulResponse<List<ServiceHealth>> web = healthClient.getHealthyServiceInstances("web");
//            System.out.println(web.getResponse().size());
        });
        stopwatch3.stop();
        System.out.println(stopwatch3);

        Stopwatch stopwatch4=Stopwatch.createStarted();
        IntStream.range(0,1000).parallel().forEach(i->{
            ConsulResponse<List<ServiceHealth>> web = healthClient.getHealthyServiceInstances("myService");
//            System.out.println(web.getResponse().size());
        });
        stopwatch4.stop();
        System.out.println(stopwatch4);

    }
}

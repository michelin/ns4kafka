package com.michelin.ns4kafka.integration;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Testcontainers
@ExtendWith(AbstractIntegrationTest.NestedSingleton.class)
public abstract class AbstractIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"));

    static RxHttpClient client;
    static EmbeddedServer server;

    static class NestedSingleton implements BeforeAllCallback, ExtensionContext.Store.CloseableResource
    {
        private static boolean initialized = false;

        @Override
        public void beforeAll(ExtensionContext context)
        {
            if (!initialized) {
                initialized = true;

                // The following line registers a callback hook when the root test context is shut down

                context.getRoot().getStore(GLOBAL).put(this.getClass().getCanonicalName(), this);

                // Your "before all tests" startup logic goes here, e.g. making connections,
                // loading in-memory DB, waiting for external resources to "warm up", etc.

                kafka.start();
                server = ApplicationContext.run(EmbeddedServer.class, PropertySource.of(
                        "test", Map.of(
                                "kafka.bootstrap.servers", kafka.getBootstrapServers(),
                                "ns4kafka.managed-clusters.test-cluster.config.bootstrap.servers", kafka.getBootstrapServers()
                        )), "test");

                client = RxHttpClient.create(server.getURL());
                System.out.println("NestedSingleton::beforeAll (setting resource)");
            }
        }

        @Override
        public void close() {
            if (!initialized) { throw new RuntimeException("Oops - this should never happen"); }

            // Cleanup the resource if needed, e.g. flush files, gracefully end connections, bury any corpses, etc.
            client.close();;
            server.close();
            kafka.close();

            System.out.println("NestedSingleton::close (clearing resource)");
        }
    }

}

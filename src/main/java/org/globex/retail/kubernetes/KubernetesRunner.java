package org.globex.retail.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.tekton.client.TektonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class KubernetesRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesRunner.class);

    @Inject
    KubernetesClient client;

    public int run() {

        TektonClient tektonClient = client.adapt(TektonClient.class);

        String tenantPrefix = System.getenv().getOrDefault("TENANT_PREFIX", "user");

        String namespace = System.getenv("NAMESPACE");
        if (namespace == null || namespace.isBlank()) {
            LOGGER.error("Environment variable 'NAMESPACE' for namespace not set. Exiting...");
            return -1;
        }

        String eventListenerName = System.getenv().getOrDefault("EVENTLISTENER", "upload-cms");

        String eventListenerService = System.getenv().getOrDefault("EVENTLISTENER_SERVICE", "http://el-" + eventListenerName + ":8080");

        String pipelineRunPrefix = System.getenv().getOrDefault("PIPELINE_RUN_PREFIX", "upload-cms-run-");

        String countStr = System.getenv("COUNT");
        if (countStr == null || countStr.isBlank()) {
            LOGGER.error("Environment variable 'COUNT' for route counter not set. Exiting...");
            return -1;
        }
        int count = Integer.parseInt(countStr);

        String countStartStr = System.getenv().getOrDefault("COUNT_START", "1");
        int countStart = Integer.parseInt(countStartStr);

        String maxTimeToWaitStr = System.getenv().getOrDefault("MAX_TIME_TO_WAIT_MS", "300000");
        String intervalStr = System.getenv().getOrDefault("INTERVAL", "5000");

        long maxTimeToWait = Long.parseLong(maxTimeToWaitStr);
        long interval = Long.parseLong(intervalStr);

        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

        //wait until pipeline is available
        String eventListenerServiceName = "el-" + eventListenerName;
        Resource<Service> service = client.services().inNamespace(namespace).withName(eventListenerServiceName);
        try {
            service.waitUntilCondition(Objects::nonNull, maxTimeToWait, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.error("Event Listener Service " + eventListenerServiceName + " is not ready after " + maxTimeToWaitStr + " milliseconds. Exiting...");
            return -1;
        }

        boolean eventListenerError = false;
        boolean pipelineRunError = false;
        for (int i = countStart; i <= count; i++) {
            try {
                URL url = new URL(eventListenerService);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("POST");
                con.setRequestProperty("Content-Type", "application/json");
                con.setRequestProperty("Accept", "application/json");
                con.setDoOutput(true);
                String jsonInputString = "{\"tenant\": \"" + tenantPrefix + i + "\"}";
                try(OutputStream os = con.getOutputStream()) {
                    byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
                int status = con.getResponseCode();
                LOGGER.info("HTTP POST call to " + url + ": statuscode " + status);
                con.disconnect();
                if (status != 202) {
                    eventListenerError = true;
                    break;
                }
            } catch (IOException e) {
                LOGGER.error("Error calling event listener.", e);
                eventListenerError = true;
                break;
            }
            String pipelineRun = pipelineRunPrefix + tenantPrefix + i;
            CountDownLatch countDownLatch = new CountDownLatch(1);
            Instant timeLimit = Instant.now().plus( Duration.ofMillis(maxTimeToWait));
            Duration untilNextRun = Duration.ofMillis(interval);
            CheckForPipelineRunTask task = new CheckForPipelineRunTask(pipelineRun, namespace, timeLimit, untilNextRun, tektonClient, ses, countDownLatch );
            ses.schedule(task,0, TimeUnit.SECONDS );

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {}

            if (Instant.now().isAfter(timeLimit)) {
                pipelineRunError = true;
                LOGGER.error("Pipelinerun " + pipelineRun + " not successful or timed out. Exiting");
                break;
            }
        }

        client.close();

        if (eventListenerError || pipelineRunError) {
            return -1;
        }

        return 0;
    }

}

package org.globex.retail.kubernetes;

import io.fabric8.knative.internal.pkg.apis.Condition;
import io.fabric8.tekton.client.TektonClient;
import io.fabric8.tekton.pipeline.v1beta1.PipelineRun;
import io.fabric8.tekton.pipeline.v1beta1.PipelineRunStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CheckForPipelineRunTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckForPipelineRunTask.class);

    private final String pipelineRunName;

    private final String namespace;

    private final Instant timeLimit;

    private final Duration untilNextRun;

    private final TektonClient tektonClient;


    private final ScheduledExecutorService scheduledExecutorService;

    private final CountDownLatch countDownLatch;

    public CheckForPipelineRunTask(String pipelineRunName, String namespace, Instant timeLimit, Duration untilNextRun, TektonClient tektonClient,
                                   ScheduledExecutorService scheduledExecutorService, CountDownLatch countDownLatch) {
        this.pipelineRunName = pipelineRunName;
        this.namespace = namespace;
        this.timeLimit = timeLimit;
        this.untilNextRun = untilNextRun;
        this.tektonClient = tektonClient;
        this.scheduledExecutorService = scheduledExecutorService;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            if (Instant.now().isAfter(timeLimit)) {
                LOGGER.error("PipelineRun with name " + pipelineRunName + " not found or not successful in namespace " + namespace + " before timeout. Exiting.");
                countDownLatch.countDown();
            }
            PipelineRun pipelineRun = getPipelineRun();
            if (pipelineRun == null) {
                LOGGER.info("PipelineRun with name " + pipelineRunName + " not found in namespace " + namespace + ". Rescheduling task.");
                this.scheduledExecutorService.schedule(this, this.untilNextRun.toSeconds(), TimeUnit.SECONDS);
            } else if (!isSucceeded(pipelineRun)) {
                LOGGER.info("PipelineRun with name " + pipelineRunName + " in namespace " + namespace + " is not succeeded. Rescheduling task.");
                this.scheduledExecutorService.schedule(this, this.untilNextRun.toSeconds(), TimeUnit.SECONDS);
            } else {
                LOGGER.info("PipelineRun with name " + pipelineRunName + " in namespace " + namespace + " is succeeded.");
                countDownLatch.countDown();
            }
        } catch (Exception e) {
            LOGGER.error("error when running task. Rescheduling task.", e);
            this.scheduledExecutorService.schedule(this, this.untilNextRun.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private PipelineRun getPipelineRun() {

        return tektonClient.v1beta1().pipelineRuns().inNamespace(namespace).withName(pipelineRunName).get();
    }

    private boolean isSucceeded(PipelineRun pipelineRun) {
        PipelineRunStatus status = pipelineRun.getStatus();
        if (status == null) {
            return false;
        }
        List<Condition> conditions = status.getConditions();
        if (conditions == null) {
            return false;
        }
        Optional<Condition> succeededCondition = conditions.stream().filter(c ->
                "Succeeded".equals(c.getType()) && "True".equals(c.getStatus())).findFirst();
        return succeededCondition.isPresent();
    }
}

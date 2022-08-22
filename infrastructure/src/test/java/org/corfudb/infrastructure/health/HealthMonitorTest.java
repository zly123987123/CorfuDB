package org.corfudb.infrastructure.health;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.health.Issue.IssueId.COMPACTION_CYCLE_FAILED;
import static org.corfudb.infrastructure.health.Issue.IssueId.CURRENT_NODE_IS_UNRESPONSIVE;
import static org.corfudb.infrastructure.health.Issue.IssueId.FAILURE_DETECTOR_TASK_FAILED;

public class HealthMonitorTest {

    @Test
    void testAddInitIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).isNotEmpty();
        assertThat(healthStatusSnapshot).containsKey(Component.COMPACTOR);
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(Component.COMPACTOR, Issue.IssueId.INIT, "Compactor is not initialized"));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.ORCHESTRATOR));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(3);
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(Component.FAILURE_DETECTOR, Issue.IssueId.INIT, "Failure Detector is not initialized"));
        assertThat(healthStatusSnapshot.get(Component.ORCHESTRATOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(Component.ORCHESTRATOR, Issue.IssueId.INIT, "Clustering Orchestrator is not initialized"));

        for (int i = 0; i < 10; i++) {
            HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        }
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).getInitHealthIssues().size()).isEqualTo(1);
    }

    @Test
    void testRemoveInitIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isInitHealthy()).isFalse();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.ORCHESTRATOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.COMPACTOR));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isInitHealthy()).isFalse();
        assertThat(healthStatusSnapshot.get(Component.ORCHESTRATOR).isInitHealthy()).isFalse();
    }

    @Test
    void testAddRuntimeIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createIssue(Component.COMPACTOR, COMPACTION_CYCLE_FAILED, "Last compaction cycle failed"));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(1);
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isRuntimeHealthy()).isFalse();

        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, "Last failure detection task failed"));
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, CURRENT_NODE_IS_UNRESPONSIVE, "Current node is in unresponsive list"));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(2);
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isRuntimeHealthy()).isFalse();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getRuntimeHealthIssues()).hasSize(2);
    }

    @Test
    void testRemoveRuntimeIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, "Last failure detection task failed"));
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, CURRENT_NODE_IS_UNRESPONSIVE, "Current node is in unresponsive list"));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getNextRuntimeIssue().get()).isEqualTo(new Issue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, "Last failure detection task failed"));
        HealthMonitor.resolveIssue(Issue.createIssue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, "Resolved"));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getNextRuntimeIssue().get()).isEqualTo(new Issue(Component.FAILURE_DETECTOR, CURRENT_NODE_IS_UNRESPONSIVE, "Current node is in unresponsive list"));
        HealthMonitor.resolveIssue(Issue.createIssue(Component.FAILURE_DETECTOR, CURRENT_NODE_IS_UNRESPONSIVE, "Resolved"));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isRuntimeHealthy()).isTrue();
    }


}

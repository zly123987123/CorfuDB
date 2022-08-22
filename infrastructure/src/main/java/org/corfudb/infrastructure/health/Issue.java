package org.corfudb.infrastructure.health;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Issue {

    enum IssueId {
        INIT,
        SEQUENCER_REQUIRES_FULL_BOOTSTRAP,
        ORCHESTRATOR_TASK_FAILED,
        FAILURE_DETECTOR_TASK_FAILED,
        CURRENT_NODE_IS_UNRESPONSIVE,
        COMPACTION_CYCLE_FAILED,
        STATE_TRANSFER_FAILED,
        QUOTA_EXCEEDED_ERROR,
        STREAM_LOG_DATA_ERROR,
    }

    @Getter
    private final Component component;
    @Getter
    private final IssueId issueId;
    @Getter
    @EqualsAndHashCode.Exclude
    private final String description;

    private static Issue issue(Component component, IssueId issueId, String description) {
        return new Issue(component, issueId, description);
    }

    public static Issue createInitIssue(Component component) {
        return Issue.issue(component, IssueId.INIT, component + " is not initialized");
    }

    public static Issue createIssue(Component component, IssueId issueId, String description) {
        return Issue.issue(component, issueId, description);
    }

    public boolean isInitIssue() {
        return issueId == IssueId.INIT;
    }

    public boolean isRuntimeIssue() {
        return !isInitIssue();
    }

}

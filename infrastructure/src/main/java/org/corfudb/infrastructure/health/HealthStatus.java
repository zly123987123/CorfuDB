package org.corfudb.infrastructure.health;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

@ToString
public class HealthStatus {

    @Getter
    private final Set<Issue> initHealthIssues;
    @Getter
    private final Set<Issue> runtimeHealthIssues;

    public HealthStatus() {
        this.initHealthIssues = new LinkedHashSet<>();
        this.runtimeHealthIssues = new LinkedHashSet<>();
    }

    public void addInitHealthIssue(Issue issue) {
        initHealthIssues.add(issue);
    }

    public void resolveInitHealthIssue(Issue issue) {
        initHealthIssues.remove(issue);
    }

    public void addRuntimeHealthIssue(Issue issue) {
        runtimeHealthIssues.add(issue);
    }

    public void resolveRuntimeHealthIssue(Issue issue) {
        runtimeHealthIssues.remove(issue);
    }

    public boolean isInitHealthy() {
        return initHealthIssues.isEmpty();
    }

    public boolean isRuntimeHealthy() {
        return runtimeHealthIssues.isEmpty();
    }

    public Optional<Issue> getNextRuntimeIssue() {
        return runtimeHealthIssues.stream().findFirst();
    }
}

package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.common.util.Tuple;

import java.util.Map;
import java.util.stream.Collectors;

@Builder
@ToString
@EqualsAndHashCode
public class HealthReport {

    @Builder.Default
    private final boolean status = false;
    @Builder.Default
    private final String reason = "Unknown";
    @NonNull
    private final ComponentReportedHealthStatus init;
    @NonNull
    private final ComponentReportedHealthStatus runtime;


    public static HealthReport fromComponentHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        Map<Component, HealthStatus> componentHealthStatusSnapshot = ImmutableMap.copyOf(componentHealthStatus);
        final ComponentReportedHealthStatus initReportedHealthStatus = createInitReportedHealthStatus(componentHealthStatusSnapshot);
        final ComponentReportedHealthStatus runtimeReportedHealthStatus = createRuntimeReportedHealthStatus(componentHealthStatusSnapshot);
        boolean overallStatus = initReportedHealthStatus.allHealthy() && runtimeReportedHealthStatus.allHealthy();
        String overallReason;
        if (!initReportedHealthStatus.hasReport()) {
            overallReason = "Status is unknown";
        }
        else if (initReportedHealthStatus.hasReport() && !initReportedHealthStatus.allHealthy()) {
            overallReason = "Some of the services are not initialized";
        } else if (runtimeReportedHealthStatus.hasReport() && !runtimeReportedHealthStatus.allHealthy()) {
            overallReason = "Some of the services experience runtime health issues";
        } else {
            overallReason = "Healthy";
        }
        return HealthReport.builder()
                .status(overallStatus)
                .reason(overallReason)
                .init(initReportedHealthStatus)
                .runtime(runtimeReportedHealthStatus)
                .build();
    }

    private static ComponentReportedHealthStatus createInitReportedHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        return ComponentReportedHealthStatus.fromMap(componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final HealthStatus healthStatus = entry.getValue();
            if (healthStatus.isInitHealthy()) {
                return Tuple.of(component, new ReportedHealthStatus(true, "Initialization successful"));
            } else {
                return Tuple.of(component, new ReportedHealthStatus(false, "Service is not initialized"));
            }
        }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second)));
    }

    private static ComponentReportedHealthStatus createRuntimeReportedHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        return ComponentReportedHealthStatus.fromMap(componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final HealthStatus healthStatus = entry.getValue();
            if (healthStatus.getLatestRuntimeIssue().isPresent()) {
                Issue issue = healthStatus.getLatestRuntimeIssue().get();
                return Tuple.of(component, new ReportedHealthStatus(false, issue.getDescription()));
            } else if (!healthStatus.isRuntimeHealthy()) {
                return Tuple.of(component, new ReportedHealthStatus(false, "Service is not running"));
            } else {
                return Tuple.of(component, new ReportedHealthStatus(true, "Up and running"));
            }
        }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second)));
    }

    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    static class ReportedHealthStatus {
        private final boolean status;
        private final String reason;
    }

    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    static class ComponentReportedHealthStatus {
        private final Map<Component, ReportedHealthStatus> report;

        public static ComponentReportedHealthStatus fromMap(Map<Component, ReportedHealthStatus> map) {
            return new ComponentReportedHealthStatus(map);
        }

        public boolean allHealthy() {
            return hasReport() && report.values().stream().allMatch(healthStatus -> healthStatus.status);
        }

        public boolean hasReport() {
            return !report.isEmpty();
        }
    }

}

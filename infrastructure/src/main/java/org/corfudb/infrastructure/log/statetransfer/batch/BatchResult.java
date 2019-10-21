package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;

/**
 * A result of a batch transfer. If completed successfully returns a batch result data,
 * if completed exceptionally, returns a batch processor failure.
 */
@AllArgsConstructor
@Getter
public class BatchResult {
    private final Result<BatchResultData, BatchProcessorFailure> result;
}
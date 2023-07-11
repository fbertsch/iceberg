/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.metacat;

import java.io.UncheckedIOException;
import java.time.Duration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;

public class S3Config {

  public static ClientOverrideConfiguration getOverrideConfig(String userAgent) {
    ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
    if (userAgent != null) {
      builder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent);
    }
    return builder.retryPolicy(getRetryPolicy()).build();
  }

  private static RetryPolicy getRetryPolicy() {
    return RetryPolicy.builder()
        .retryCondition(OrRetryCondition.create(
            RetryCondition.defaultRetryCondition(), RetryOnExceptionsCondition.create(UncheckedIOException.class))
        )
        .backoffStrategy(FullJitterBackoffStrategy.builder()
            .baseDelay(Duration.ofSeconds(1))
            .maxBackoffTime(Duration.ofMinutes(3))
            .build())
        .numRetries(15)
        .build();
  }
}

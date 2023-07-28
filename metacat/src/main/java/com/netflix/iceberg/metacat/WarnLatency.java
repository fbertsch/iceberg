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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import org.slf4j.Logger;

public class WarnLatency {

  private final long latencyThresholdMs;

  private final Logger logger;

  private final String description;

  public static Builder builder() {
    return new Builder();
  }

  private WarnLatency(long latencyThresholdMs, Logger logger, String description) {
    this.latencyThresholdMs = latencyThresholdMs;
    this.logger = logger;
    this.description = description;
  }

  public <V> V call(Callable<V> callable) {
    V result;
    Instant start = Instant.now();
    try {
      result = callable.call();
      check(start);
      return result;
    } catch (RuntimeException e) {
      check(start, e);
      throw e;
    } catch (Exception e) {
      check(start, e);
      throw new RuntimeException(e);
    }
  }

  public void run(Runnable runnable) {
    Instant start = Instant.now();
    try {
      runnable.run();
      check(start);
    } catch (Throwable e) {
      check(start, e);
      throw e;
    }
  }

  private void check(Instant start, Throwable e) {
    long latencyMs = Duration.between(start, Instant.now()).toMillis();
    if (latencyMs > latencyThresholdMs) {
      logger.warn("Took {} ms to {}", latencyMs, description, e);
    }
  }

  private void check(Instant start) {
    long latencyMs = Duration.between(start, Instant.now()).toMillis();
    if (latencyMs > latencyThresholdMs) {
      logger.warn("Took {} ms to {}", latencyMs, description);
    }
  }

  public static final class Builder {
    private long latencyThresholdMs;
    private Logger logger;
    private String description;

    private Builder() {}

    public Builder withThreshold(long latencyThresholdMs) {
      this.latencyThresholdMs = latencyThresholdMs;
      return this;
    }

    public Builder withLogger(Logger logger) {
      this.logger = logger;
      return this;
    }

    public Builder withDescription(String format, Object... args) {
      this.description = String.format(format, args);
      return this;
    }

    public WarnLatency build() {
      return new WarnLatency(latencyThresholdMs, logger, description);
    }
  }
}

/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.splunk.conditions;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.it.conditions.ConditionCheck;
import com.google.cloud.teleport.it.splunk.SplunkResourceManager;
import javax.annotation.Nullable;

/** ConditionCheck to validate if Splunk has received a certain amount of events. */
@AutoValue
public abstract class SplunkEventsCheck extends ConditionCheck {

  abstract SplunkResourceManager resourceManager();

  @Nullable
  abstract String query();

  abstract Integer minEvents();

  @Nullable
  abstract Integer maxEvents();

  @Override
  public String getDescription() {
    if (maxEvents() != null) {
      return String.format(
          "Splunk check if logs have between %d and %d events", minEvents(), maxEvents());
    }
    return String.format("Splunk check if logs have %d events", minEvents());
  }

  @Override
  public CheckResult check() {
    long totalEvents;
    if (query() != null) {
      totalEvents = resourceManager().getEvents(query()).size();
    } else {
      totalEvents = resourceManager().getEvents().size();
    }
    if (totalEvents < minEvents()) {
      return new CheckResult(
          false, String.format("Expected %d but has only %d", minEvents(), totalEvents));
    }
    if (maxEvents() != null && totalEvents > maxEvents()) {
      return new CheckResult(
          false, String.format("Expected up to %d but found %d events", maxEvents(), totalEvents));
    }

    if (maxEvents() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d events and found %d",
              minEvents(), maxEvents(), totalEvents));
    }

    return new CheckResult(
        true, String.format("Expected at least %d events and found %d", minEvents(), totalEvents));
  }

  public static Builder builder(SplunkResourceManager resourceManager) {
    return new AutoValue_SplunkEventsCheck.Builder().setResourceManager(resourceManager);
  }

  /** Builder for {@link SplunkEventsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(SplunkResourceManager resourceManager);

    public abstract Builder setQuery(String query);

    public abstract Builder setMinEvents(Integer minEvents);

    public abstract Builder setMaxEvents(Integer maxEvents);

    abstract SplunkEventsCheck autoBuild();

    public SplunkEventsCheck build() {
      return autoBuild();
    }
  }
}

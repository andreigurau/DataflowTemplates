/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.bigquery;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utilities for {@link com.google.cloud.teleport.it.bigquery.BigQueryResourceManager}
 * implementations.
 */
public final class BigQueryResourceManagerUtils {
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

  private BigQueryResourceManagerUtils() {}

  static String generateDatasetId(String datasetName) {
    // Letters, numbers, and underscores only allowed. max length of 1024. Add check for these
    checkArgument(datasetName.length() != 0, "baseString cannot be empty!");

    LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of("UTC"));

    return datasetName + "_" + localDateTime.format(DATE_FORMAT) + "_" + System.nanoTime();
  }

  // maybe a validateTableId function?
}

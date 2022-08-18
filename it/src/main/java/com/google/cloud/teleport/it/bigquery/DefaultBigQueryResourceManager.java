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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default class for implementation of {@link BigQueryResourceManager} interface. */
public final class DefaultBigQueryResourceManager implements BigQueryResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigQueryResourceManager.class);

  private final String projectId;
  private final String region;

  private final BigQuery bigQuery;
  private Dataset dataset;
  private String datasetName;

  DefaultBigQueryResourceManager(
      BigQuery bigQuery, String projectId, String region) {
    this.bigQuery = bigQuery;
    this.projectId = projectId;
    this.region = region;
  }

  private DefaultBigQueryResourceManager(Builder builder) {
    this(
        BigQueryOptions.newBuilder().setProjectId(builder.projectId).build().getService(),
        builder.projectId,
        builder.region
    );
  }

  public static Builder builder(String projectId, String region) {
    return new Builder(projectId, region);
  }

  private void checkForDataset() throws IllegalStateException {
    if(dataset == null) {
      throw new IllegalStateException("Dataset for integration tests has not been created. Please create a dataset first using the 'createDataset()' function");
    }
  }

  private Table getTableIfExists(String tableName) throws IllegalStateException {
    Table table = dataset.get(tableName);
    if(table == null) {
      throw new IllegalStateException("Table " + tableName + " has not been created. Please create a table first using the 'createTable()' function");
    }
    return table;
  }

  private void logInsertErrors(Map<Long,List<BigQueryError>> insertErrors) {
    for(Map.Entry<Long, List<BigQueryError>> entries : insertErrors.entrySet()) {
      long index = entries.getKey();
      for(BigQueryError error : entries.getValue()) {
        LOG.info("Error when inserting row with index {}: {}", index, error.getMessage());
      }
    }
  }

  @Override
  public void createDataset(String datasetName) throws Exception {
    try {
      String fullDatasetName = BigQueryResourceManagerUtils.generateDatasetId(datasetName);
      if (dataset != null) {
        LOG.info("Already using dataset {}", this.datasetName);
        return;
      }


      DatasetInfo datasetInfo = DatasetInfo.newBuilder(fullDatasetName).setLocation(this.region).build();
      this.dataset = bigQuery.create(datasetInfo);
      this.datasetName = dataset.getDatasetId().getDataset();
      LOG.info("Dataset {} created successfully", datasetName);

    } catch (Exception e) {
      throw new Exception("Failed to create dataset: " + e.getMessage());
    }
  }

  @Override
  public void createTable(String tableName, Schema schema) throws Exception {
    checkForDataset();
    try {
      TableId tableId = TableId.of(datasetName, tableName);
      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
      bigQuery.create(tableInfo);
      LOG.info("{}.{} table created successfully", datasetName, tableName);
    } catch (Exception e) {
      throw new Exception("Failed to create table: " + e.getMessage());
    }
  }

  @Override
  public void write(String tableName, RowToInsert row) throws Exception {
    write(tableName, ImmutableList.of(row));
  }

  @Override
  public void write(String tableName, List<RowToInsert> rows) throws Exception {
    checkForDataset();
    Table table = getTableIfExists(tableName);
    try {
      LOG.info("Attempting to write {} records to {}.{}.", rows.size(), datasetName, tableName);
      int successfullyWrittenRecords = rows.size();
      InsertAllResponse insertResponse = table.insert(rows);
      successfullyWrittenRecords -= insertResponse.getInsertErrors().size();
      LOG.info("Successfully written {} records to {}.{}.", successfullyWrittenRecords, datasetName, tableName);
      if(insertResponse.hasErrors()) {
        LOG.info("Errors encountered when inserting rows: ");
        logInsertErrors(insertResponse.getInsertErrors());
      }
    } catch (Exception e) {
      throw new Exception("Failed to write to table: " + e.getMessage());
    }
  }

  @Override
  public ImmutableList<FieldValueList> readTableRecords(String tableName) throws Exception {
    checkForDataset();
    Table table = getTableIfExists(tableName);
    try {
      LOG.info("Loading all records from {}.{}", datasetName, tableName);
      TableResult results = bigQuery.listTableData(table.getTableId());
      ImmutableList.Builder<FieldValueList> immutableListBuilder = ImmutableList.builder();
      for(FieldValueList row : results.getValues()) {
        immutableListBuilder.add(row);
      }
      ImmutableList<FieldValueList> tableRecords = immutableListBuilder.build();
      LOG.info(
          "Loaded {} records from {}.{}", tableRecords.size(), datasetName, tableName);
      return tableRecords;
    } catch (Exception e) {
      throw new Exception("Failed to read from table: " + e.getMessage());
    }
  }

  @Override
  public void cleanup() throws Exception {
    LOG.info("Attempting to cleanup manager.");
    try {
      bigQuery.delete(dataset.getDatasetId());
    } catch (Exception e) {
      throw new Exception("Failed to delete resources: " + e.getMessage());
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link DefaultBigQueryResourceManager}. */
  public static final class Builder {
    
    private final String projectId;
    private final String region;

    private Builder(String projectId, String region) {
      this.projectId = projectId;
      this.region = region;
    }

    public DefaultBigQueryResourceManager build() {
      return new DefaultBigQueryResourceManager(this);
    }
  }
}

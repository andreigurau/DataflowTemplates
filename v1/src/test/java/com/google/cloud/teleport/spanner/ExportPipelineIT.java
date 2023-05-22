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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatGenericRecords;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.Artifact;
import com.google.cloud.teleport.it.gcp.artifacts.utils.AvroTestUtil;
import com.google.cloud.teleport.it.gcp.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/** Integration test for {@link ExportPipeline} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(ExportPipeline.class)
@RunWith(JUnit4.class)
public class ExportPipelineIT extends TemplateTestBase {

  private SpannerResourceManager googleSqlResourceManager;
  private SpannerResourceManager postgresResourceManager;

  private static final String EMPTY_TABLE_SCHEMA_JSON =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"EmptyTable\",\n"
          + "  \"namespace\": \"spannerexport\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"id\",\n"
          + "      \"type\": \"long\",\n"
          + "      \"sqlType\": \"INT64\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  private static final String SINGERS_SCHEMA_JSON =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"Singers\",\n"
          + "  \"namespace\": \"spannerexport\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"Id\",\n"
          + "      \"type\": \"long\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"FirstName\",\n"
          + "      \"type\": \"string\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"LastName\",\n"
          + "      \"type\": \"string\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  private static final Schema EMPTY_TABLE_SCHEMA =
      new Schema.Parser().parse(EMPTY_TABLE_SCHEMA_JSON);
  private static final Schema SINGERS_SCHEMA = new Schema.Parser().parse(SINGERS_SCHEMA_JSON);

  @Before
  public void setUp() throws IOException {
    googleSqlResourceManager =
        SpannerResourceManager.builder(
                testName + "-googleSql", PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .build();
    postgresResourceManager =
        SpannerResourceManager.builder(testName + "-postgres", PROJECT, REGION, Dialect.POSTGRESQL)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(googleSqlResourceManager, postgresResourceManager);
  }

  private List<Mutation> getRecordsToWrite() {
    return ImmutableList.of(
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("Id")
            .to(1)
            .set("FirstName")
            .to("Roger")
            .set("LastName")
            .to("Waters")
            .build(),
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("Id")
            .to(2)
            .set("FirstName")
            .to("Nick")
            .set("LastName")
            .to("Mason")
            .build(),
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("Id")
            .to(3)
            .set("FirstName")
            .to("David")
            .set("LastName")
            .to("Gilmour")
            .build(),
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("Id")
            .to(4)
            .set("FirstName")
            .to("Richard")
            .set("LastName")
            .to("Wright")
            .build());
  }

  private List<Map<String, Object>> getExpectedRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(ImmutableMap.of("Id", 1, "FirstName", "Roger", "LastName", "Waters"));
    expectedRows.add(ImmutableMap.of("Id", 2, "FirstName", "Nick", "LastName", "Mason"));
    expectedRows.add(ImmutableMap.of("Id", 3, "FirstName", "David", "LastName", "Gilmour"));
    expectedRows.add(ImmutableMap.of("Id", 4, "FirstName", "Richard", "LastName", "Wright"));
    return expectedRows;
  }

  @Test
  public void testGoogleSqlExportPipeline() throws IOException {
    // Arrange
    String createEmptyTableStatement =
        "CREATE TABLE EmptyTable (\n" + "  id INT64 NOT NULL,\n" + ") PRIMARY KEY(id)";
    googleSqlResourceManager.createTable(createEmptyTableStatement);

    String createSingersTableStatement =
        "CREATE TABLE Singers (\n"
            + "  Id INT64,\n"
            + "  FirstName STRING(MAX),\n"
            + "  LastName STRING(MAX),\n"
            + ") PRIMARY KEY(Id)";
    googleSqlResourceManager.createTable(createSingersTableStatement);
    googleSqlResourceManager.write(getRecordsToWrite());
    baseExportPipeline(
        googleSqlResourceManager.getInstanceId(), googleSqlResourceManager.getDatabaseId());
  }

  @Test
  public void testPostgresExportPipeline() throws IOException {
    // Arrange
    String createEmptyTableStatement =
        "CREATE TABLE \"EmptyTable\" (\n" + "  id bigint NOT NULL,\nPRIMARY KEY(id)\n" + ")";
    postgresResourceManager.createTable(createEmptyTableStatement);

    String createSingersTableStatement =
        "CREATE TABLE \"Singers\" (\n"
            + "  \"Id\" bigint,\n"
            + "  \"FirstName\" character varying(256),\n"
            + "  \"LastName\" character varying(256),\n"
            + "PRIMARY KEY(\"Id\"))";
    postgresResourceManager.createTable(createSingersTableStatement);
    postgresResourceManager.write(getRecordsToWrite());
    baseExportPipeline(
        postgresResourceManager.getInstanceId(), postgresResourceManager.getDatabaseId());
  }

  private void baseExportPipeline(String instanceId, String databaseId) throws IOException {
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("instanceId", instanceId)
            .addParameter("databaseId", databaseId)
            .addParameter("outputDir", getGcsPath("output/"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> emptyTableArtifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*EmptyTable.avro.*"));

    List<GenericRecord> emptyTableRecords = new ArrayList<>();
    for (Artifact artifact : emptyTableArtifacts) {
      try {
        emptyTableRecords.addAll(AvroTestUtil.readRecords(EMPTY_TABLE_SCHEMA, artifact.contents()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading " + artifact.name() + " as Avro.", e);
      }
    }
    assertThatGenericRecords(emptyTableRecords).hasRows(0);

    List<Artifact> singersArtifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*Singers.avro.*"));

    List<GenericRecord> singersRecords = new ArrayList<>();
    for (Artifact artifact : singersArtifacts) {
      try {
        singersRecords.addAll(AvroTestUtil.readRecords(SINGERS_SCHEMA, artifact.contents()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading " + artifact.name() + " as Avro.", e);
      }
    }
    assertThatGenericRecords(singersRecords).hasRecordsUnordered(getExpectedRows());
  }
}

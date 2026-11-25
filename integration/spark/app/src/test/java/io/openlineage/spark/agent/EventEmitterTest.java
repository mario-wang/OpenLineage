/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import lombok.SneakyThrows;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

class EventEmitterTest {
  @SneakyThrows
  @Test
  void marksDefaultAndExplicitNamespaceWithoutChangingIt() {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    HttpConfig transport = new HttpConfig();
    transport.setUrl(URI.create("http://localhost:8090"));
    transport.setEndpoint("/api/lineage");
    config.setTransportConfig(transport);
    OpenLineage.RunEvent original =
        OpenLineageClientUtils.runEventFromJson(
            "{\"eventTime\":\"2026-11-23T00:00:00Z\",\"eventType\":\"COMPLETE\","
                + "\"producer\":\"https://example.com\","
                + "\"run\":{\"runId\":\"018c7e39-96d7-75db-b483-747d1158cb31\"},"
                + "\"job\":{\"namespace\":\"default\",\"name\":\"job\"},"
                + "\"inputs\":[],\"outputs\":[]}");

    EventEmitter emitter = new EventEmitter(config, "job");
    JsonNode marked =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(OpenLineageClientUtils.toJson(emitter.markNamespaceOrigin(original)));
    assertThat(marked.path("job").path("namespace").asText()).isEqualTo("default");
    assertThat(marked.path("job").path("facets").path("namespace_origin").path("default").asBoolean())
        .isTrue();

    config.setNamespace("default");
    EventEmitter explicit = new EventEmitter(config, "job");
    JsonNode explicitEvent =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(OpenLineageClientUtils.toJson(explicit.markNamespaceOrigin(original)));
    assertThat(explicitEvent.path("job").path("facets").path("namespace_origin").path("default").asBoolean())
        .isFalse();
  }

  @SneakyThrows
  @Test
  void testApplicationNameOverride() {
    String appName = "test-app";
    String overrideName = "override-app";

    SparkConf sparkConf =
        new SparkConf().setAppName(appName).set(ArgumentParser.SPARK_CONF_APP_NAME, overrideName);
    SparkOpenLineageConfig olConfig = ArgumentParser.parse(sparkConf);
    EventEmitter eventEmitter = new EventEmitter(olConfig, appName);

    assertThat(eventEmitter.getApplicationJobName()).isEqualTo(overrideName);
  }

  @SneakyThrows
  @Test
  void testApplicationJobNameCanBeUpdatedAfterConstruction() {
    String appName = "raw-spark-app-name";

    SparkConf sparkConf = new SparkConf().setAppName(appName);
    SparkOpenLineageConfig olConfig = ArgumentParser.parse(sparkConf);
    EventEmitter eventEmitter = new EventEmitter(olConfig, appName);

    assertThat(eventEmitter.getApplicationJobName()).isEqualTo(appName);

    String resolvedName = "glue_job_name";
    eventEmitter.setApplicationJobName(resolvedName);

    assertThat(eventEmitter.getApplicationJobName()).isEqualTo(resolvedName);
  }
}

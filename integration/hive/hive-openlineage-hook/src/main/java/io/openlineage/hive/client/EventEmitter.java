/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import static io.openlineage.client.utils.UUIDUtils.generateStaticUUID;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.hive.api.OpenLineageContext;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;

@Getter
@Slf4j
public class EventEmitter implements AutoCloseable {
  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();
  private final OpenLineageClient client;
  private final UUID runId;
  private final String jobName;
  private final String jobNamespace;
  private final boolean namespaceDefault;

  public EventEmitter(OpenLineageContext olContext) {
    Configuration conf = olContext.getHookContext().getConf();
    this.client = Clients.newClient(olContext.getOpenLineageConfig());
    this.runId = getRunId(olContext);
    this.namespaceDefault = conf.getRaw(HiveOpenLineageConfigParser.NAMESPACE_KEY) == null;
    this.jobNamespace = conf.get(HiveOpenLineageConfigParser.NAMESPACE_KEY, "default");
    this.jobName = conf.get(HiveOpenLineageConfigParser.JOB_NAME_KEY, getJobName(olContext));
  }


  public void emit(RunEvent event) {
    try {
      client.emit(markNamespaceOrigin(event, namespaceDefault));
      log.debug(
          "Emitting lineage completed successfully: {}", OpenLineageClientUtils.toJson(event));
    } catch (OpenLineageClientException | IOException exception) {
      log.error("Could not emit lineage", exception);
    }
  }

  static RunEvent markNamespaceOrigin(RunEvent event, boolean namespaceDefault) throws IOException {
    ObjectNode tree = (ObjectNode) MAPPER.readTree(OpenLineageClientUtils.toJson(event));
    ObjectNode marker = tree.with("job").with("facets").putObject("namespace_origin");
    marker.put("_producer", "https://github.com/OpenLineage/OpenLineage/integration/hive");
    marker.put("_schemaURL", "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobFacet");
    marker.put("integration", "HIVE");
    marker.put("default", namespaceDefault);
    return OpenLineageClientUtils.runEventFromJson(MAPPER.writeValueAsString(tree));
  }

  public static String getJobName(OpenLineageContext olContext) {
    return olContext.getHookContext().getOperationName();
  }

  /*
   * Get the same runId for START and STOP events
   */
  public static UUID getRunId(OpenLineageContext olContext) {
    HookContext hookContext = olContext.getHookContext();
    // QueryInfo is populated only by HiveServer2, it is null when running via Hive CLI
    // https://github.com/apache/hive/blob/rel/release-3.1.3/ql/src/java/org/apache/hadoop/hive/ql/QueryInfo.java
    QueryInfo queryInfo = hookContext.getQueryInfo();
    if (queryInfo != null && queryInfo.getOperationId() != null) {
      return generateStaticUUID(
          Instant.ofEpochMilli(queryInfo.getBeginTime()),
          queryInfo.getOperationId().getBytes(UTF_8));
    }

    QueryPlan queryPlan = hookContext.getQueryPlan();
    if (queryPlan != null
        && queryPlan.getQueryId() != null
        && queryPlan.getQueryStartTime() != null) {
      return generateStaticUUID(
          Instant.ofEpochMilli(queryPlan.getQueryStartTime()),
          queryPlan.getQueryId().getBytes(UTF_8));
    }

    log.warn("Unable to determine query id and start time, generating random runId");
    return UUIDUtils.generateNewUUID();
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}

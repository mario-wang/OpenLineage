/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.hive.api.OpenLineageContext;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.junit.jupiter.api.Test;

class EventEmitterTest {

  @Test
  void marksNamespaceOriginWithoutRewritingTheJob() throws Exception {
    OpenLineage.RunEvent original =
        OpenLineageClientUtils.runEventFromJson(
            "{\"eventTime\":\"2026-11-23T00:00:00Z\",\"eventType\":\"COMPLETE\","
                + "\"producer\":\"https://example.com\","
                + "\"run\":{\"runId\":\"018c7e39-96d7-75db-b483-747d1158cb31\"},"
                + "\"job\":{\"namespace\":\"default\",\"name\":\"job\"},"
                + "\"inputs\":[],\"outputs\":[]}");
    String marked = OpenLineageClientUtils.toJson(EventEmitter.markNamespaceOrigin(original, true));
    assertThat(marked).contains("\"namespace\":\"default\"");
    assertThat(marked).contains("\"namespace_origin\"");
    assertThat(marked).contains("\"default\":true");
  }

  @Test
  void testRunIdFromQueryInfo() {
    HookContext hookContext = mock(HookContext.class);
    QueryInfo queryInfo = mock(QueryInfo.class);
    when(queryInfo.getBeginTime()).thenReturn(1_700_000_000_000L);
    when(queryInfo.getOperationId()).thenReturn("operation-id");
    when(hookContext.getQueryInfo()).thenReturn(queryInfo);

    UUID runId = EventEmitter.getRunId(context(hookContext));

    assertThat(runId).isEqualTo(EventEmitter.getRunId(context(hookContext)));
    assertThat(runId.version()).isEqualTo(7);
  }

  @Test
  void testRunIdFromQueryPlanWhenQueryInfoIsMissing() {
    // Hive CLI does not populate QueryInfo
    HookContext hookContext = mock(HookContext.class);
    QueryPlan queryPlan = mock(QueryPlan.class);
    when(queryPlan.getQueryId()).thenReturn("hive_20260101000000_query-id");
    when(queryPlan.getQueryStartTime()).thenReturn(1_700_000_000_000L);
    when(hookContext.getQueryInfo()).thenReturn(null);
    when(hookContext.getQueryPlan()).thenReturn(queryPlan);

    UUID runId = EventEmitter.getRunId(context(hookContext));

    assertThat(runId).isEqualTo(EventEmitter.getRunId(context(hookContext)));
    assertThat(runId.version()).isEqualTo(7);
  }

  @Test
  void testRandomRunIdWhenNoQueryDataAvailable() {
    HookContext hookContext = mock(HookContext.class);

    assertThat(EventEmitter.getRunId(context(hookContext))).isNotNull();
  }

  private OpenLineageContext context(HookContext hookContext) {
    return OpenLineageContext.builder()
        .openLineage(new OpenLineage(URI.create("http://test-producer")))
        .openLineageConfig(new HiveOpenLineageConfig())
        .hookContext(hookContext)
        .readEntities(Collections.emptySet())
        .writeEntities(Collections.emptySet())
        .eventTime(ZonedDateTime.now())
        .eventType(OpenLineage.RunEvent.EventType.START)
        .build();
  }
}

/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import org.junit.jupiter.api.Test;

class JobNamespaceOriginTest {
  @Test
  void marksDefaultWithoutChangingJobNamespace() {
    OpenLineage ol = new OpenLineage(URI.create("https://example.com/flink"));
    OpenLineage.JobFacetsBuilder facets = ol.newJobFacetsBuilder();
    JobNamespaceOrigin.mark(ol, facets, true);
    OpenLineage.Job job = ol.newJobBuilder().namespace("flink_jobs").name("job").facets(facets.build()).build();
    String json = OpenLineageClientUtils.toJson(job);
    assertTrue(json.contains("\"namespace\":\"flink_jobs\""));
    assertTrue(json.contains("\"namespace_origin\""));
    assertTrue(json.contains("\"default\":true"));
  }
}

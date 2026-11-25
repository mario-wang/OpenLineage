/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import io.openlineage.client.OpenLineage;

/** Adds namespace configuration provenance for a lineage processor. */
public final class JobNamespaceOrigin {
  private JobNamespaceOrigin() {}

  public static void mark(
      OpenLineage ol, OpenLineage.JobFacetsBuilder facets, boolean namespaceDefault) {
    OpenLineage.JobFacet marker = ol.newJobFacet();
    marker.getAdditionalProperties().put("integration", "FLINK");
    marker.getAdditionalProperties().put("default", namespaceDefault);
    facets.put("namespace_origin", marker);
  }
}

/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.spark.agent.lance.LanceCatalogHandler;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.ClickHouseHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.DatabricksDeltaHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.DatabricksUnityV2Handler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.DeltaHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.JdbcHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.SupportsNamespacesCatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler;
import java.util.List;

/** Catalog handlers available to every Spark 3.x / 4.x version. */
final class DefaultCatalogHandlers {
  private DefaultCatalogHandlers() {}

  static List<CatalogHandler> list(OpenLineageContext context) {
    // Specific catalogs must precede broad fallback handlers such as SupportsNamespaces.
    return ImmutableList.of(
        new LanceCatalogHandler(),
        new IcebergHandler(context),
        new DeltaHandler(context),
        new ClickHouseHandler(context),
        new DatabricksDeltaHandler(context),
        new DatabricksUnityV2Handler(context),
        new JdbcHandler(context),
        new SupportsNamespacesCatalogHandler(context));
  }
}

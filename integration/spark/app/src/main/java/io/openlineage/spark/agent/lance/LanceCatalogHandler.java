/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lance;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Identifies Lance REST tables by their catalog endpoint and table path. */
public final class LanceCatalogHandler implements CatalogHandler {
  private static final String LANCE_CATALOG_CLASS = "org.lance.spark.LanceNamespaceSparkCatalog";

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog catalog) {
    return LANCE_CATALOG_CLASS.equals(catalog.getClass().getName());
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {
    String uri = session.conf().get("spark.sql.catalog." + catalog.name() + ".uri", "").trim();
    if (uri.isEmpty()) {
      throw new MissingDatasetIdentifierCatalogException("Lance REST URI is missing");
    }

    StringJoiner name = new StringJoiner(".");
    for (String part : identifier.namespace()) {
      name.add(part);
    }
    name.add(identifier.name());
    return new DatasetIdentifier(name.toString(), uri);
  }

  @Override
  public String getName() {
    return "lance";
  }
}

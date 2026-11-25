/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.util.Collections;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;

class LanceCatalogHandlerTest {
  private final LanceCatalogHandler handler = new LanceCatalogHandler();

  @Test
  void identifiesRestTableFromCatalogConfiguration() {
    SparkSession session = mock(SparkSession.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    TableCatalog catalog = mock(TableCatalog.class);
    when(session.conf()).thenReturn(conf);
    when(catalog.name()).thenReturn("lance");
    when(conf.get("spark.sql.catalog.lance.uri", "")).thenReturn("http://lance:9101/lance");

    DatasetIdentifier dataset =
        handler.getDatasetIdentifier(
            session,
            catalog,
            Identifier.of(new String[] {"spark_db"}, "orders"),
            Collections.emptyMap());

    assertThat(dataset.getNamespace()).isEqualTo("http://lance:9101/lance");
    assertThat(dataset.getName()).isEqualTo("spark_db.orders");
    assertThat(dataset.getSymlinks()).isEmpty();
    verify(conf).get("spark.sql.catalog.lance.uri", "");
    verifyNoMoreInteractions(conf);
  }

  @Test
  void rejectsMissingRestSource() {
    SparkSession session = mock(SparkSession.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    TableCatalog catalog = mock(TableCatalog.class);
    when(session.conf()).thenReturn(conf);
    when(catalog.name()).thenReturn("lance");
    when(conf.get("spark.sql.catalog.lance.uri", "")).thenReturn("");

    assertThatThrownBy(
            () ->
                handler.getDatasetIdentifier(
                    session,
                    catalog,
                    Identifier.of(new String[] {"spark_db"}, "orders"),
                    Collections.emptyMap()))
        .isInstanceOf(MissingDatasetIdentifierCatalogException.class);
  }
}

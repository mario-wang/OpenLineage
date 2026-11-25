/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.util.CatalogDatasetFacetUtils;
import io.openlineage.spark.agent.util.GoogleCloudPlatformUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class SupportsNamespacesCatalogHandlerTest {
  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final SupportsNamespacesCatalogHandler catalogHandler =
      new SupportsNamespacesCatalogHandler(context);
  private final V2SessionCatalog catalog = mock(V2SessionCatalog.class);
  private final SparkSession session = mock(SparkSession.class);
  private final SparkContext sparkContext = mock(SparkContext.class);
  private final Map<String, String> properties =
      Collections.singletonMap("location", "file:/tmp/warehouse/schema.db/table");
  private final Map<String, String> namespaceMetadata =
      Collections.singletonMap("location", "file:/tmp/warehouse/schema.db");
  private final Identifier id = Identifier.of(new String[] {"catalog", "schema"}, "table");

  @BeforeEach
  void setUp() {
    when(context.getSparkSession()).thenReturn(Optional.of(session));
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration(false));
  }

  @Test
  void supportsAnyNamespaceCatalog() {
    TableCatalog namespaceCatalog =
        mock(TableCatalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));

    assertThat(catalogHandler.isClass(namespaceCatalog)).isTrue();
  }

  @Test
  void addsHiveMetastoreSymlinkForHiveNamespaceCatalog() {
    TableCatalog hiveCatalog = mock(ExampleHiveCatalog.class);
    Identifier identifier = Identifier.of(new String[] {"sales"}, "orders");
    SparkSession hiveSession = mock(SparkSession.class);
    SparkContext hiveSparkContext = mock(SparkContext.class);
    URI metastoreUri = URI.create("thrift://hive-host:9083");
    when(hiveSession.sparkContext()).thenReturn(hiveSparkContext);
    when(hiveSparkContext.hadoopConfiguration()).thenReturn(new Configuration(false));

    try (MockedStatic<CatalogDatasetFacetUtils> catalogUtils =
            mockStatic(CatalogDatasetFacetUtils.class);
        MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class)) {
      catalogUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(hiveSparkContext))
          .thenReturn(true);
      pathUtils
          .when(() -> PathUtils.fromPath(any()))
          .thenReturn(new DatasetIdentifier("/warehouse/sales/orders", "file"));
      pathUtils
          .when(() -> PathUtils.getMetastoreUri(hiveSparkContext))
          .thenReturn(Optional.of(metastoreUri));
      pathUtils
          .when(() -> PathUtils.prepareHiveUri(metastoreUri))
          .thenReturn(URI.create("hive://hive-host:9083"));

      DatasetIdentifier dataset =
          catalogHandler.getDatasetIdentifier(
              hiveSession,
              hiveCatalog,
              identifier,
              Collections.singletonMap(TableCatalog.PROP_LOCATION, "file:/warehouse/sales/orders"));

      assertThat(dataset.getSymlinks()).hasSize(1);
      assertThat(dataset.getSymlinks().get(0).getNamespace()).isEqualTo("hive://hive-host:9083");
    }
  }

  private interface ExampleHiveCatalog extends TableCatalog, SupportsNamespaces {}

  @Test
  void testGetNonHiveDatasetIdentifierWhenOnlyTableLocationPresent() {
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    // no warehouse -> no symlinks
    assertThat(datasetIdentifier.getSymlinks()).hasSize(0);
  }

  @Test
  void testGetNonHiveDatasetIdentifierWhenWarehouseLocationPresent() {
    when(sparkContext.getConf())
        .thenReturn(new SparkConf().set("spark.sql.warehouse.dir", "file:/tmp/warehouse"));
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);

    DatasetIdentifier datasetIdentifier =
        catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

    assertThat(datasetIdentifier)
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "schema.table")
        .hasFieldOrPropertyWithValue("namespace", "file:/tmp/warehouse")
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }

  @Test
  void droppedTableUsesDatabaseDirectoryOnceAndSameIdentityAsWrites() {
    when(sparkContext.getConf())
        .thenReturn(new SparkConf().set("spark.sql.warehouse.dir", "file:/tmp/warehouse"));
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);
    DatasetIdentifier written =
        catalogHandler.getDatasetIdentifier(session, catalog, id, properties);
    DatasetIdentifier dropped =
        catalogHandler.getDatasetIdentifier(session, catalog, id, Collections.emptyMap());
    assertThat(dropped.getName()).isEqualTo(written.getName());
    assertThat(dropped.getNamespace()).isEqualTo(written.getNamespace());
    assertThat(dropped.getSymlinks()).usingRecursiveComparison().isEqualTo(written.getSymlinks());
  }

  @Test
  void droppedTableUsesCustomDatabaseDirectory() {
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(catalog.loadNamespaceMetadata(id.namespace()))
        .thenReturn(Collections.singletonMap("location", "file:/custom/database"));
    DatasetIdentifier dataset =
        catalogHandler.getDatasetIdentifier(session, catalog, id, Collections.emptyMap());
    assertThat(dataset.getName()).isEqualTo("/custom/database/table");
    assertThat(dataset.getSymlinks()).isEmpty();
  }

  @Test
  void testGetHiveDatasetIdentifier() {
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(catalog))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);
      try (MockedStatic<PathUtils> pathUtils = mockStatic(PathUtils.class)) {
        URI hiveUri = URI.create("hive://hive-host:9093");
        pathUtils
            .when(() -> PathUtils.getMetastoreUri(sparkContext))
            .thenReturn(Optional.of(hiveUri));
        pathUtils.when(() -> PathUtils.prepareHiveUri(hiveUri)).thenReturn(hiveUri);
        pathUtils
            .when(() -> PathUtils.fromPath(any()))
            .thenReturn(new DatasetIdentifier("/tmp/warehouse/schema.db/table", "file"));

        DatasetIdentifier datasetIdentifier =
            catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

        assertThat(datasetIdentifier)
            .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
            .hasFieldOrPropertyWithValue("namespace", "file");

        assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

        assertThat(datasetIdentifier.getSymlinks().get(0))
            .hasFieldOrPropertyWithValue("name", "catalog.schema.table")
            .hasFieldOrPropertyWithValue("namespace", "hive://hive-host:9093")
            .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
      }
    }
  }

  @Test
  void testGetLakehouseHiveDatasetIdentifier() {
    when(sparkContext.getConf())
        .thenReturn(new SparkConf().set("spark.sql.catalogImplementation", "hive"));
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(namespaceMetadata);

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(catalog))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);
      try (MockedStatic<GoogleCloudPlatformUtils> gcpUtils =
          mockStatic(GoogleCloudPlatformUtils.class)) {
        gcpUtils.when(() -> GoogleCloudPlatformUtils.isBigLakeHiveCatalog(any())).thenReturn(true);

        DatasetIdentifier datasetIdentifier =
            catalogHandler.getDatasetIdentifier(session, catalog, id, properties);

        assertThat(datasetIdentifier)
            .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/schema.db/table")
            .hasFieldOrPropertyWithValue("namespace", "file");

        assertThat(datasetIdentifier.getSymlinks()).hasSize(1);

        assertThat(datasetIdentifier.getSymlinks().get(0))
            .hasFieldOrPropertyWithValue("name", "catalog.schema.table")
            .hasFieldOrPropertyWithValue("namespace", "gcp_lakehouse")
            .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
      }
    }
  }

  @Test
  void testGetCatalogFacetForNonHive() {
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(catalog))
          .thenReturn(false);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);

      Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
          catalogHandler.getCatalogDatasetFacet(mock(V2SessionCatalog.class), new HashMap<>());
      assertThat(catalogDatasetFacet).isEmpty();
    }
  }

  @Test
  void testGetCatalogFacetForHive() {
    OpenLineage.CatalogDatasetFacet cf = mock(OpenLineage.CatalogDatasetFacet.class);

    try (MockedStatic<CatalogDatasetFacetUtils> catalogDatasetUtils =
        mockStatic(CatalogDatasetFacetUtils.class)) {
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveCatalog(any()))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.isHiveSupportEnabled(sparkContext))
          .thenReturn(true);
      catalogDatasetUtils
          .when(() -> CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(any()))
          .thenReturn(Optional.of(cf));
      Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
          catalogHandler.getCatalogDatasetFacet(mock(V2SessionCatalog.class), new HashMap<>());
      assertThat(catalogDatasetFacet).isNotEmpty();
      assertThat(catalogDatasetFacet.get().getCatalogDatasetFacet()).isEqualTo(cf);
    }
  }

  @Test
  void testGetDatasetIdentifierWhenNoTableLocationPresent() {
    when(catalog.loadNamespaceMetadata(id.namespace())).thenReturn(Collections.emptyMap());

    OpenLineageClientException thrown =
        assertThrows(
            OpenLineageClientException.class,
            () -> catalogHandler.getDatasetIdentifier(session, catalog, id, new HashMap<>()));

    assertThat(thrown.getMessage())
        .contains("Unable to extract DatasetIdentifier from namespace catalog");
  }
}

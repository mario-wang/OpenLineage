/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SqlDatasetExtractorTest {
  @Test
  void keepsFlinkCatalogIdentity() {
    SqlDatasetExtractor extractor = new SqlDatasetExtractor();

    assertEquals(
        Arrays.asList("hive_alias", "sales.orders"), extractor.identity("hive_alias", "sales", "orders"));
  }
}

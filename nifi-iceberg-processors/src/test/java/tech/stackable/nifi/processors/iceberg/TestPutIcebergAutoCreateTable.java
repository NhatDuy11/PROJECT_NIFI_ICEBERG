/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.stackable.nifi.processors.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static tech.stackable.nifi.processors.iceberg.PutIceberg.ICEBERG_RECORD_COUNT;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.CATALOG_NAME;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.validateData;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static tech.stackable.nifi.services.iceberg.AbstractCatalogService.S3_AWS_CREDENTIALS_PROVIDER_SERVICE;
import static tech.stackable.nifi.services.iceberg.AbstractCatalogService.WAREHOUSE_LOCATION;
import static tech.stackable.nifi.services.iceberg.IcebergHiveCatalogService.METASTORE_URI;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.nifi.hive.metastore.ThriftMetastore;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.extension.RegisterExtension;
import tech.stackable.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils;
import tech.stackable.nifi.services.iceberg.IcebergHiveCatalogService;

/**
 * Test class for PutIceberg processor with auto-create table functionality using Hive Metastore and
 * S3 storage.
 */
@DisabledOnOs(WINDOWS)
public class TestPutIcebergAutoCreateTable extends AbstractTestPutIceberg {

  @RegisterExtension public static ThriftMetastore metastore = new ThriftMetastore();

  private static final String AUTO_CREATE_TABLE_NAME = "auto_created_users";
  private static final TableIdentifier AUTO_CREATE_TABLE_IDENTIFIER =
      TableIdentifier.of(CATALOG_NAME, AUTO_CREATE_TABLE_NAME);

  /**
   * Initializes the catalog service with Hive Metastore and S3 configuration for auto-create table
   * testing.
   */
  private void initCatalogForAutoCreate() throws InitializationException {
    // Setup AWS Credentials Provider with real-like test credentials
    final AWSCredentialsProviderService credentialsProvider =
        new AWSCredentialsProviderControllerService();
    runner.addControllerService("credentials-provider", credentialsProvider);
    runner.setProperty(credentialsProvider, "Access Key ID", "AKIAIOSFODNN7EXAMPLE");
    runner.setProperty(
        credentialsProvider, "Secret Access Key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    runner.enableControllerService(credentialsProvider);

    // Setup Hive Catalog Service with S3 endpoint
    final IcebergHiveCatalogService catalogService = new IcebergHiveCatalogService();
    runner.addControllerService("catalog-service", catalogService);
    runner.setProperty(catalogService, S3_AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-provider");
    runner.setProperty(catalogService, METASTORE_URI, metastore.getThriftConnectionUri());
    runner.setProperty(catalogService, WAREHOUSE_LOCATION, "s3a://test-bucket/warehouse/");

    // Add S3 endpoint configuration as dynamic property
    runner.setProperty(catalogService, "fs.s3a.endpoint", "http://192.168.1.156:9000");
    runner.setProperty(catalogService, "fs.s3a.path.style.access", "true");
    runner.setProperty(catalogService, "fs.s3a.connection.ssl.enabled", "false");
    runner.setProperty(catalogService, "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    runner.enableControllerService(catalogService);

    // Create catalog factory and set catalog reference
    final IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
    catalog = catalogFactory.create();

    runner.setProperty(PutIceberg.CATALOG, "catalog-service");
  }

  /**
   * Test auto-create table functionality with Avro input and Parquet output format. This test
   * verifies that: 1. Table is automatically created when it doesn't exist 2. Schema is correctly
   * inferred from Avro input 3. Data is written in Parquet format 4. All records are processed
   * successfully
   */
  @Test
  public void testAutoCreateTableWithAvroInputParquetOutput() throws Exception {
    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalogForAutoCreate();

    // Verify table doesn't exist initially
    boolean tableExists = true;
    try {
      catalog.loadTable(AUTO_CREATE_TABLE_IDENTIFIER);
    } catch (NoSuchTableException e) {
      tableExists = false;
    }
    assertFalse(tableExists, "Table should not exist before auto-create test");

    // Configure processor for auto-create with Parquet format
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
    runner.setProperty(PutIceberg.TABLE_NAME, AUTO_CREATE_TABLE_NAME);
    runner.setProperty(PutIceberg.AUTO_CREATE_TABLE, "true");
    runner.setProperty(PutIceberg.FILE_FORMAT, "PARQUET");
    runner.setProperty(PutIceberg.UNMATCHED_COLUMN_BEHAVIOR, "FAIL_UNMATCHED_COLUMN");

    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0]);
    runner.run();

    // Verify table was created and data was written
    final Table table = catalog.loadTable(AUTO_CREATE_TABLE_IDENTIFIER);
    assertTrue(table != null, "Table should be auto-created");

    // Verify table properties
    assertEquals("PARQUET", table.properties().get(TableProperties.DEFAULT_FILE_FORMAT));
    assertTrue(table.spec().isUnpartitioned(), "Auto-created table should be unpartitioned");

    // Verify expected records
    final List<Record> expectedRecords =
        IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
            .add(0, "John", "Finance")
            .add(1, "Jill", "Finance")
            .add(2, "James", "Marketing")
            .add(3, "Joana", "Sales")
            .build();

    // Verify processing results
    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
    final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);
    assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));

    // Validate data and file structure
    validateData(table, expectedRecords, 0);
    validateNumberOfDataFiles(new URI(table.location()).getPath(), 1);
  }

  /**
   * Test auto-create table functionality with ORC output format. This test verifies that the file
   * format specified in processor configuration is correctly applied to the auto-created table.
   */
  @Test
  public void testAutoCreateTableWithOrcFormat() throws Exception {
    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalogForAutoCreate();

    final String orcTableName = "auto_created_users_orc";
    final TableIdentifier orcTableIdentifier = TableIdentifier.of(CATALOG_NAME, orcTableName);

    // Configure processor for auto-create with ORC format
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
    runner.setProperty(PutIceberg.TABLE_NAME, orcTableName);
    runner.setProperty(PutIceberg.AUTO_CREATE_TABLE, "true");
    runner.setProperty(PutIceberg.FILE_FORMAT, "ORC");

    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0]);
    runner.run();

    // Verify table was created with correct format
    final Table table = catalog.loadTable(orcTableIdentifier);
    assertEquals("ORC", table.properties().get(TableProperties.DEFAULT_FILE_FORMAT));

    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
    final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);
    assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));

    // Cleanup
    catalog.dropTable(orcTableIdentifier);
  }

  /** Test that processor works normally when auto-create is disabled and table already exists. */
  @Test
  public void testNormalOperationWithAutoCreateDisabled() throws Exception {
    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalogForAutoCreate();

    // Pre-create table manually
    final Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.AVRO.name());
    catalog.createTable(
        AUTO_CREATE_TABLE_IDENTIFIER,
        USER_SCHEMA,
        org.apache.iceberg.PartitionSpec.unpartitioned(),
        tableProperties);

    // Configure processor with auto-create disabled
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
    runner.setProperty(PutIceberg.TABLE_NAME, AUTO_CREATE_TABLE_NAME);
    runner.setProperty(PutIceberg.AUTO_CREATE_TABLE, "false");
    runner.setProperty(PutIceberg.FILE_FORMAT, "AVRO");

    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0]);
    runner.run();

    // Verify normal operation
    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
    final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);
    assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
  }

  /** Test that processor fails gracefully when auto-create is disabled and table doesn't exist. */
  @Test
  public void testFailureWhenTableNotExistsAndAutoCreateDisabled() throws Exception {
    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalogForAutoCreate();

    final String nonExistentTable = "non_existent_table";

    // Configure processor with auto-create disabled for non-existent table
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
    runner.setProperty(PutIceberg.TABLE_NAME, nonExistentTable);
    runner.setProperty(PutIceberg.AUTO_CREATE_TABLE, "false");

    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0]);
    runner.run();

    // Verify failure
    runner.assertTransferCount(PutIceberg.REL_FAILURE, 1);
    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 0);
  }

  /** Test auto-create with expression language for dynamic table names. */
  @Test
  public void testAutoCreateWithExpressionLanguage() throws Exception {
    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalogForAutoCreate();

    // Configure processor with expression language
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "${catalog.namespace}");
    runner.setProperty(PutIceberg.TABLE_NAME, "${table.name}");
    runner.setProperty(PutIceberg.AUTO_CREATE_TABLE, "true");
    runner.setProperty(PutIceberg.FILE_FORMAT, "PARQUET");

    // Set attributes for expression language
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("catalog.namespace", CATALOG_NAME);
    attributes.put("table.name", "dynamic_auto_created_table");

    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0], attributes);
    runner.run();

    // Verify table was created with dynamic name
    final TableIdentifier dynamicTableId =
        TableIdentifier.of(CATALOG_NAME, "dynamic_auto_created_table");
    final Table table = catalog.loadTable(dynamicTableId);
    assertTrue(table != null, "Dynamic table should be auto-created");

    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);

    // Cleanup
    catalog.dropTable(dynamicTableId);
  }

  @Override
  public void tearDown() {
    // Clean up auto-created tables
    try {
      if (catalog != null) {
        catalog.dropTable(AUTO_CREATE_TABLE_IDENTIFIER);
      }
    } catch (Exception e) {
      // Ignore cleanup errors
    }
    super.tearDown();
  }
}

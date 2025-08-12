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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static tech.stackable.nifi.processors.iceberg.PutIceberg.ICEBERG_RECORD_COUNT;
import static tech.stackable.nifi.processors.iceberg.PutIceberg.ICEBERG_SNAPSHOT_SUMMARY_FLOWFILE_UUID;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.CATALOG_NAME;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.validateData;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils.validatePartitionFolders;
import static tech.stackable.nifi.services.iceberg.AbstractCatalogService.S3_AWS_CREDENTIALS_PROVIDER_SERVICE;
import static tech.stackable.nifi.services.iceberg.AbstractCatalogService.WAREHOUSE_LOCATION;
import static tech.stackable.nifi.services.iceberg.IcebergHiveCatalogService.METASTORE_URI;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.nifi.hive.metastore.ThriftMetastore;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.extension.RegisterExtension;
import tech.stackable.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import tech.stackable.nifi.processors.iceberg.util.IcebergTestUtils;
import tech.stackable.nifi.services.iceberg.IcebergHiveCatalogService;

@DisabledOnOs(WINDOWS)
public class TestPutIcebergWithHiveCatalog extends AbstractTestPutIceberg {

  @RegisterExtension public static ThriftMetastore metastore = new ThriftMetastore();

  private void initCatalog(PartitionSpec spec, FileFormat fileFormat)
      throws InitializationException {
    final Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());

    final AWSCredentialsProviderService credentialsProvider =
        new AWSCredentialsProviderControllerService();
    runner.addControllerService("credentials-provider", credentialsProvider);
    runner.setProperty(credentialsProvider, "Access Key ID", "my-access-key");
    runner.setProperty(credentialsProvider, "Secret Access Key", "my-secret-key");
    runner.enableControllerService(credentialsProvider);

    final IcebergHiveCatalogService catalogService = new IcebergHiveCatalogService();
    runner.addControllerService("catalog-service", catalogService);
    runner.setProperty(catalogService, S3_AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-provider");
    runner.setProperty(catalogService, METASTORE_URI, metastore.getThriftConnectionUri());
    runner.setProperty(catalogService, WAREHOUSE_LOCATION, warehousePath);
    runner.enableControllerService(catalogService);

    final IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
    catalog = catalogFactory.create();

    catalog.createTable(TABLE_IDENTIFIER, USER_SCHEMA, spec, tableProperties);

    runner.setProperty(PutIceberg.CATALOG, "catalog-service");
  }

  @Test
  public void onTriggerIdentityPartitioned() throws Exception {
    final PartitionSpec spec = PartitionSpec.builderFor(USER_SCHEMA).identity("department").build();

    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalog(spec, FileFormat.ORC);
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
    runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0]);
    runner.run();

    final Table table = catalog.loadTable(TABLE_IDENTIFIER);

    final List<Record> expectedRecords =
        IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
            .add(0, "John", "Finance")
            .add(1, "Jill", "Finance")
            .add(2, "James", "Marketing")
            .add(3, "Joana", "Sales")
            .build();

    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
    final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

    final String tableLocation = new URI(table.location()).getPath();
    assertTrue(table.spec().isPartitioned());
    assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
    validateData(table, expectedRecords, 0);
    validateNumberOfDataFiles(tableLocation, 3);
    validatePartitionFolders(
        tableLocation,
        Arrays.asList("department=Finance", "department=Marketing", "department=Sales"));
    assertProvenanceEvents();
  }

  @Test
  public void onTriggerMultiLevelIdentityPartitioned() throws Exception {
    final PartitionSpec spec =
        PartitionSpec.builderFor(USER_SCHEMA).identity("name").identity("department").build();

    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalog(spec, FileFormat.PARQUET);
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
    runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
    runner.setValidateExpressionUsage(false);
    runner.enqueue(new byte[0]);
    runner.run();

    final Table table = catalog.loadTable(TABLE_IDENTIFIER);

    final List<Record> expectedRecords =
        IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
            .add(0, "John", "Finance")
            .add(1, "Jill", "Finance")
            .add(2, "James", "Marketing")
            .add(3, "Joana", "Sales")
            .build();

    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
    final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

    final String tableLocation = new URI(table.location()).getPath();
    assertTrue(table.spec().isPartitioned());
    assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
    validateData(table, expectedRecords, 0);
    validateNumberOfDataFiles(tableLocation, 4);
    validatePartitionFolders(
        tableLocation,
        Arrays.asList(
            "name=James/department=Marketing/",
            "name=Jill/department=Finance/",
            "name=Joana/department=Sales/",
            "name=John/department=Finance/"));
    assertProvenanceEvents();
  }

  @Test
  public void onTriggerUnPartitioned() throws Exception {
    runner = TestRunners.newTestRunner(processor);
    initRecordReader();
    initCatalog(PartitionSpec.unpartitioned(), FileFormat.AVRO);
    runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "${catalog.name}");
    runner.setProperty(PutIceberg.TABLE_NAME, "${table.name}");
    runner.setProperty(PutIceberg.MAXIMUM_FILE_SIZE, "${max.filesize}");
    runner.setProperty("snapshot-property.additional-summary-property", "test summary property");
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("catalog.name", CATALOG_NAME);
    attributes.put("table.name", TABLE_NAME);
    attributes.put("max.filesize", "536870912"); // 512 MB
    runner.enqueue(new byte[0], attributes);
    runner.run();

    final Table table = catalog.loadTable(TABLE_IDENTIFIER);

    final List<Record> expectedRecords =
        IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
            .add(0, "John", "Finance")
            .add(1, "Jill", "Finance")
            .add(2, "James", "Marketing")
            .add(3, "Joana", "Sales")
            .build();

    runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
    final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

    assertTrue(table.spec().isUnpartitioned());
    assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
    validateData(table, expectedRecords, 0);
    validateNumberOfDataFiles(new URI(table.location()).getPath(), 1);
    assertProvenanceEvents();
    assertSnapshotSummaryProperties(
        table, Collections.singletonMap("additional-summary-property", "test summary property"));
  }

  private void assertProvenanceEvents() {
    final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
    assertEquals(1, provenanceEvents.size());
    final ProvenanceEventRecord sendEvent = provenanceEvents.get(0);
    assertEquals(ProvenanceEventType.SEND, sendEvent.getEventType());
    assertTrue(sendEvent.getTransitUri().endsWith(CATALOG_NAME + ".db/" + TABLE_NAME));
  }

  private void assertSnapshotSummaryProperties(Table table, Map<String, String> summaryProperties) {
    final Map<String, String> snapshotSummary = table.currentSnapshot().summary();

    assertTrue(snapshotSummary.containsKey(ICEBERG_SNAPSHOT_SUMMARY_FLOWFILE_UUID));

    for (Map.Entry<String, String> entry : summaryProperties.entrySet()) {
      assertEquals(snapshotSummary.get(entry.getKey()), entry.getValue());
    }
  }
}

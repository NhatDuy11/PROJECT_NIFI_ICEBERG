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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;

/**
 * Utility class for automatically creating Iceberg tables based on NiFi record schemas. Converts
 * NiFi record schemas to Iceberg schemas and creates tables with appropriate properties.
 */
public class IcebergTableCreator {

  /**
   * Creates an Iceberg table if it doesn't exist, using the provided record schema.
   *
   * @param catalog The Iceberg catalog to use for table creation
   * @param tableIdentifier The identifier for the table to create
   * @param recordSchema The NiFi record schema to convert to Iceberg schema
   * @param fileFormat The default file format for the table (PARQUET, AVRO, ORC)
   * @param logger Logger for debugging and info messages
   * @return The created or existing table
   * @throws Exception if table creation fails
   */
  public static Table createTableIfNotExists(
      Catalog catalog,
      TableIdentifier tableIdentifier,
      RecordSchema recordSchema,
      String fileFormat,
      ComponentLog logger)
      throws Exception {

    try {
      // Try to load existing table first
      return catalog.loadTable(tableIdentifier);
    } catch (Exception e) {
      logger.info(
          "Table {} does not exist, attempting to create it automatically", tableIdentifier);

      // Convert NiFi RecordSchema to Iceberg Schema
      Schema icebergSchema = convertRecordSchemaToIcebergSchema(recordSchema, logger);

      // Create table properties
      Map<String, String> tableProperties = createDefaultTableProperties(fileFormat);

      // Create unpartitioned table (can be extended later for partitioning support)
      PartitionSpec spec = PartitionSpec.unpartitioned();

      try {
        Table table = catalog.createTable(tableIdentifier, icebergSchema, spec, tableProperties);
        logger.info("Successfully created Iceberg table: {}", tableIdentifier);
        return table;
      } catch (AlreadyExistsException aee) {
        // Table was created by another process, load it
        logger.info(
            "Table {} was created by another process, loading existing table", tableIdentifier);
        return catalog.loadTable(tableIdentifier);
      }
    }
  }

  /**
   * Converts a NiFi RecordSchema to an Iceberg Schema.
   *
   * @param recordSchema The NiFi record schema to convert
   * @param logger Logger for debugging messages
   * @return The converted Iceberg schema
   */
  private static Schema convertRecordSchemaToIcebergSchema(
      RecordSchema recordSchema, ComponentLog logger) {
    List<Types.NestedField> fields =
        recordSchema.getFields().stream()
            .map(field -> convertRecordFieldToIcebergField(field, logger))
            .collect(Collectors.toList());

    return new Schema(fields);
  }

  /**
   * Converts a single NiFi RecordField to an Iceberg NestedField.
   *
   * @param recordField The NiFi record field to convert
   * @param logger Logger for debugging messages
   * @return The converted Iceberg nested field
   */
  private static Types.NestedField convertRecordFieldToIcebergField(
      RecordField recordField, ComponentLog logger) {
    String fieldName = recordField.getFieldName();
    DataType dataType = recordField.getDataType();
    boolean isOptional = recordField.isNullable();

    // Generate unique field ID (simple incremental approach)
    int fieldId = Math.abs(fieldName.hashCode()) % 10000 + 1;

    Type icebergType = convertDataTypeToIcebergType(dataType, fieldName, logger);

    if (isOptional) {
      return Types.NestedField.optional(fieldId, fieldName, icebergType);
    } else {
      return Types.NestedField.required(fieldId, fieldName, icebergType);
    }
  }

  /**
   * Converts NiFi DataType to Iceberg Type.
   *
   * @param dataType The NiFi data type to convert
   * @param fieldName The field name for error reporting
   * @param logger Logger for debugging messages
   * @return The converted Iceberg type
   */
  private static Type convertDataTypeToIcebergType(
      DataType dataType, String fieldName, ComponentLog logger) {
    RecordFieldType fieldType = dataType.getFieldType();

    switch (fieldType) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case BYTE:
      case SHORT:
      case INT:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
      case CHAR:
        return Types.StringType.get();
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Types.TimeType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case DECIMAL:
        // Check if this is actually an integer (Oracle NUMBER without decimal places)
        if (dataType instanceof org.apache.nifi.serialization.record.type.DecimalDataType) {
          org.apache.nifi.serialization.record.type.DecimalDataType decimalType =
              (org.apache.nifi.serialization.record.type.DecimalDataType) dataType;

          // If scale is 0, treat as integer type for better performance
          if (decimalType.getScale() == 0) {
            // Use BIGINT for Oracle NUMBER without decimal places
            logger.info(
                "Converting Oracle NUMBER field '{}' to BIGINT (precision: {}, scale: 0)",
                fieldName,
                decimalType.getPrecision());
            return Types.LongType.get(); // BIGINT in Iceberg
          }
        }

        // For actual decimal numbers, keep as decimal
        return Types.DecimalType.of(38, 18);
      case ARRAY:
        // Handle array types
        DataType elementType = ((ArrayDataType) dataType).getElementType();
        Type elementIcebergType =
            convertDataTypeToIcebergType(elementType, fieldName + "_element", logger);
        return Types.ListType.ofOptional(fieldId++, elementIcebergType);
      case RECORD:
        // Handle nested record types
        logger.warn("Nested record types are not fully supported yet for field: {}", fieldName);
        return Types.StringType.get(); // Fallback to string
      case MAP:
        // Handle map types
        logger.warn("Map types are not fully supported yet for field: {}", fieldName);
        return Types.StringType.get(); // Fallback to string
      case CHOICE:
        // Handle union types
        logger.warn("Choice/Union types are not fully supported yet for field: {}", fieldName);
        return Types.StringType.get(); // Fallback to string
      default:
        logger.warn(
            "Unsupported data type {} for field {}, using STRING as fallback",
            fieldType,
            fieldName);
        return Types.StringType.get();
    }
  }

  /**
   * Creates default table properties for the Iceberg table.
   *
   * @param fileFormat The default file format (PARQUET, AVRO, ORC)
   * @return Map of table properties
   */
  private static Map<String, String> createDefaultTableProperties(String fileFormat) {
    Map<String, String> properties = new HashMap<>();

    // Set default file format
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toLowerCase());

    // Set other useful defaults
    properties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "536870912"); // 512 MB
    properties.put(TableProperties.SPLIT_SIZE, "268435456"); // 256 MB

    // Enable format version 2 for better performance
    properties.put(TableProperties.FORMAT_VERSION, "2");

    return properties;
  }

  // Static field ID counter to ensure unique IDs
  private static int fieldId = 1;
}

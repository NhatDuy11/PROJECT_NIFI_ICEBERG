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
package tech.stackable.nifi.processors.iceberg.converter;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

public class ArrayElementGetter {

  private static final String ARRAY_FIELD_NAME = "array element";

  /**
   * Creates an accessor for getting elements in an internal array data structure at the given
   * position.
   *
   * @param dataType the element type of the array
   */
  public static ElementGetter createElementGetter(DataType dataType) {
    ElementGetter elementGetter;
    switch (dataType.getFieldType()) {
      case STRING:
        elementGetter = element -> DataTypeUtils.toString(element, ARRAY_FIELD_NAME);
        break;
      case CHAR:
        elementGetter = element -> DataTypeUtils.toCharacter(element, ARRAY_FIELD_NAME);
        break;
      case BOOLEAN:
        elementGetter = element -> DataTypeUtils.toBoolean(element, ARRAY_FIELD_NAME);
        break;
      case DECIMAL:
        elementGetter = element -> DataTypeUtils.toBigDecimal(element, ARRAY_FIELD_NAME);
        break;
      case BYTE:
        elementGetter = element -> DataTypeUtils.toByte(element, ARRAY_FIELD_NAME);
        break;
      case SHORT:
        elementGetter = element -> DataTypeUtils.toShort(element, ARRAY_FIELD_NAME);
        break;
      case INT:
        elementGetter = element -> DataTypeUtils.toInteger(element, ARRAY_FIELD_NAME);
        break;
      case DATE:
        elementGetter =
            element ->
                DataTypeUtils.toLocalDate(
                    element, () -> DateTimeFormatter.ISO_LOCAL_DATE, ARRAY_FIELD_NAME);
        break;
      case TIME:
        elementGetter =
            element ->
                DataTypeUtils.toTime(
                    element, () -> new SimpleDateFormat("HH:mm:ss"), ARRAY_FIELD_NAME);
        break;
      case LONG:
        elementGetter = element -> DataTypeUtils.toLong(element, ARRAY_FIELD_NAME);
        break;
      case BIGINT:
        elementGetter = element -> DataTypeUtils.toBigInt(element, ARRAY_FIELD_NAME);
        break;
      case FLOAT:
        elementGetter = element -> DataTypeUtils.toFloat(element, ARRAY_FIELD_NAME);
        break;
      case DOUBLE:
        elementGetter = element -> DataTypeUtils.toDouble(element, ARRAY_FIELD_NAME);
        break;
      case TIMESTAMP:
        elementGetter =
            element -> {
              if (element == null) {
                return null;
              }

              String raw = element.toString().trim();

              List<DateTimeFormatter> formatters =
                  List.of(
                      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
                      DateTimeFormatter.ofPattern("dd-MMM-yy hh.mm.ss.SSSSSSSSS a", Locale.ENGLISH),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS"),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSS"),
                      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss"));

              for (DateTimeFormatter formatter : formatters) {
                try {
                  return java.time.LocalDateTime.parse(raw, formatter);
                } catch (DateTimeParseException ignored) {

                } catch (Exception e) {

                  System.err.println(
                      "Unexpected error parsing timestamp with formatter "
                          + formatter
                          + ": "
                          + e.getMessage());
                }
              }

              throw new IllegalArgumentException("Unparseable timestamp: " + raw);
            };
        break;

      case ENUM:
        elementGetter =
            element -> DataTypeUtils.toEnum(element, (EnumDataType) dataType, ARRAY_FIELD_NAME);
        break;
      case UUID:
        elementGetter = DataTypeUtils::toUUID;
        break;
      case ARRAY:
        elementGetter =
            element ->
                DataTypeUtils.toArray(
                    element, ARRAY_FIELD_NAME, ((ArrayDataType) dataType).getElementType());
        break;
      case MAP:
        elementGetter = element -> DataTypeUtils.toMap(element, ARRAY_FIELD_NAME);
        break;
      case RECORD:
        elementGetter = element -> DataTypeUtils.toRecord(element, ARRAY_FIELD_NAME);
        break;
      case CHOICE:
        elementGetter =
            element -> {
              final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
              final DataType chosenDataType = DataTypeUtils.chooseDataType(element, choiceDataType);
              if (chosenDataType == null) {
                throw new IllegalTypeConversionException(
                    String.format(
                        "Cannot convert value [%s] of type %s for array element to any of the following available Sub-Types for a Choice: %s",
                        element, element.getClass(), choiceDataType.getPossibleSubTypes()));
              }

              return DataTypeUtils.convertType(element, chosenDataType, ARRAY_FIELD_NAME);
            };
        break;
      default:
        throw new IllegalArgumentException("Unsupported field type: " + dataType.getFieldType());
    }

    return element -> {
      if (element == null) {
        return null;
      }

      return elementGetter.getElementOrNull(element);
    };
  }

  /** Accessor for getting the elements of an array during runtime. */
  public interface ElementGetter extends Serializable {
    @Nullable
    Object getElementOrNull(Object element);
  }
}

/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata.DefaultCharset;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author <a href="mailto:ahmedahamid@yahoo.com">Ahmed Abdul Hamid</a>
 */
public class TableMapEventMetadataDeserializer {

    private final Logger logger = Logger.getLogger(getClass().getName());

    public TableMapEventMetadata deserialize(ByteArrayInputStream inputStream, int nColumns, byte[] columnTypes)
        throws IOException {

        int remainingBytes = inputStream.available();
        if (remainingBytes <= 0) {
            return null;
        }

        TableMapEventMetadata result = new TableMapEventMetadata();

        for (; remainingBytes > 0; inputStream.enterBlock(remainingBytes)) {
            int code = inputStream.readInteger(1);

            MetadataFieldType fieldType = MetadataFieldType.byCode(code);
            if (fieldType == null) {
                throw new IOException("Unsupported table metadata field type " + code);
            }
            if (MetadataFieldType.UNKNOWN_METADATA_FIELD_TYPE.equals(fieldType)) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Received metadata field of unknown type");
                }
                continue;
            }

            //for some reasons, the UNKNOWN_METADATA_FIELD_TYPE will mess up the stream
            if(inputStream.available() == 0) {
                logger.warning("Stream is empty so cannot read field length for field type: " + fieldType);
                return result;
            }

            int fieldLength = inputStream.readPackedInteger();

            remainingBytes = inputStream.available();
            inputStream.enterBlock(fieldLength);

            switch (fieldType) {
                case SIGNEDNESS:
                    int numericColumns = 0;
                    BitSet bitSet = new BitSet();
                    for (int i = 0; i < columnTypes.length; i++) {
                        switch (ColumnType.byCode(columnTypes[i] & 0xff)) {
                            case TINY:
                            case SHORT:
                            case INT24:
                            case LONG:
                            case LONGLONG:
                            case NEWDECIMAL:
                            case FLOAT:
                            case DOUBLE:
                            case YEAR:
                                numericColumns++;
                                bitSet.set(i);
                                break;
                            default:
                                break;
                        }
                    }
                    BitSet signednessBitSet = readBooleanList(inputStream, numericColumns);
                    BitSet finalBitSet = new BitSet();

                    for ( int i = 0, j = 0; i < columnTypes.length; i++) {
                        if ( bitSet.get(i) ) // if is numeric
                            bitSet.set(i, signednessBitSet.get(j++)); // set signed-ness
                    }
                    result.setSignedness(bitSet);
                    break;
                case DEFAULT_CHARSET:
                    result.setDefaultCharset(readDefaultCharset(inputStream));
                    break;
                case COLUMN_CHARSET:
                    result.setColumnCharsets(readIntegers(inputStream));
                    break;
                case COLUMN_NAME:
                    result.setColumnNames(readColumnNames(inputStream));
                    break;
                case SET_STR_VALUE:
                    result.setSetStrValues(readTypeValues(inputStream));
                    break;
                case ENUM_STR_VALUE:
                    result.setEnumStrValues(readTypeValues(inputStream));
                    break;
                case GEOMETRY_TYPE:
                    result.setGeometryTypes(readIntegers(inputStream));
                    break;
                case SIMPLE_PRIMARY_KEY:
                    result.setSimplePrimaryKeys(readIntegers(inputStream));
                    break;
                case PRIMARY_KEY_WITH_PREFIX:
                    result.setPrimaryKeysWithPrefix(readIntegerPairs(inputStream));
                    break;
                case ENUM_AND_SET_DEFAULT_CHARSET:
                    result.setEnumAndSetDefaultCharset(readDefaultCharset(inputStream));
                    break;
                case ENUM_AND_SET_COLUMN_CHARSET:
                    result.setEnumAndSetColumnCharsets(readIntegers(inputStream));
                    break;
                case VISIBILITY:
                    result.setVisibility(readBooleanList(inputStream, nColumns));
                    break;
                default:
                    inputStream.enterBlock(remainingBytes);
                    throw new IOException("Unsupported table metadata field type " + code);
            }
            remainingBytes -= fieldLength;
        }
        return result;
    }

    public TableMapEventMetadata deserialize(BinaryLogEventDataReader eventDataReader, int nColumns, byte[] columnTypes)
        throws IOException {

        int remainingBytes = eventDataReader.available();
        if (remainingBytes == 0) {
            return null;
        }

        TableMapEventMetadata result = new TableMapEventMetadata();

        for (; remainingBytes > 0; eventDataReader.enterBlock(remainingBytes)) {
            int code = eventDataReader.readUnsignedByte();

            MetadataFieldType fieldType = MetadataFieldType.byCode(code);
            if (fieldType == null) {
                throw new IOException("Unsupported table metadata field type " + code);
            }
            if (MetadataFieldType.UNKNOWN_METADATA_FIELD_TYPE.equals(fieldType)) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Received metadata field of unknown type");
                }
                continue;
            }

            // for some reasons, the UNKNOWN_METADATA_FIELD_TYPE will mess up the stream
            if (eventDataReader.available() == 0) {
                logger.warning("Buffer is empty so cannot read field length for field type: " + fieldType);
                return result;
            }

            int fieldLength = eventDataReader.readPackedInteger();

            remainingBytes = eventDataReader.available();
            eventDataReader.enterBlock(fieldLength);

            switch (fieldType) {
                case SIGNEDNESS:
                    int numericColumns = 0;
                    BitSet bitSet = new BitSet(columnTypes.length);
                    for (int i = 0; i < columnTypes.length; i++) {
                        switch (ColumnType.byCode(columnTypes[i] & 0xff)) {
                            case TINY:
                            case SHORT:
                            case INT24:
                            case LONG:
                            case LONGLONG:
                            case NEWDECIMAL:
                            case FLOAT:
                            case DOUBLE:
                            case YEAR:
                                numericColumns++;
                                bitSet.set(i);
                                break;
                            default:
                                break;
                        }
                    }
                    BitSet signednessBitSet = readBooleanList(eventDataReader, numericColumns);

                    for (int i = 0, j = 0; i < columnTypes.length; i++) {
                        if (bitSet.get(i)) {
                            bitSet.set(i, signednessBitSet.get(j++)); // set signed-ness
                        }
                    }
                    result.setSignedness(bitSet);
                    break;
                case DEFAULT_CHARSET:
                    result.setDefaultCharset(readDefaultCharset(eventDataReader));
                    break;
                case COLUMN_CHARSET:
                    result.setColumnCharsets(readIntegers(eventDataReader));
                    break;
                case COLUMN_NAME:
                    result.setColumnNames(readColumnNames(eventDataReader));
                    break;
                case SET_STR_VALUE:
                    result.setSetStrValues(readTypeValues(eventDataReader));
                    break;
                case ENUM_STR_VALUE:
                    result.setEnumStrValues(readTypeValues(eventDataReader));
                    break;
                case GEOMETRY_TYPE:
                    result.setGeometryTypes(readIntegers(eventDataReader));
                    break;
                case SIMPLE_PRIMARY_KEY:
                    result.setSimplePrimaryKeys(readIntegers(eventDataReader));
                    break;
                case PRIMARY_KEY_WITH_PREFIX:
                    result.setPrimaryKeysWithPrefix(readIntegerPairs(eventDataReader));
                    break;
                case ENUM_AND_SET_DEFAULT_CHARSET:
                    result.setEnumAndSetDefaultCharset(readDefaultCharset(eventDataReader));
                    break;
                case ENUM_AND_SET_COLUMN_CHARSET:
                    result.setEnumAndSetColumnCharsets(readIntegers(eventDataReader));
                    break;
                case VISIBILITY:
                    result.setVisibility(readBooleanList(eventDataReader, nColumns));
                    break;
                default:
                    eventDataReader.enterBlock(remainingBytes);
                    throw new IOException("Unsupported table metadata field type " + code);
            }
            remainingBytes -= fieldLength;
        }
        return result;
    }


    private static BitSet readBooleanList(ByteArrayInputStream inputStream, int length) throws IOException {
        BitSet result = new BitSet();
        // according to MySQL internals the amount of storage required for N columns is INT((N+7)/8) bytes
        byte[] bytes = inputStream.read((length + 7) >> 3);
        for (int i = 0; i < length; ++i) {
            if ((bytes[i >> 3] & (1 << (7 - (i % 8)))) != 0) {
                result.set(i);
            }
        }
        return result;
    }

    private static BitSet readBooleanList(BinaryLogEventDataReader eventDataReader, int length) {
        byte[] bytes = eventDataReader.readBytes((length + 7) >> 3);

        // Bit reversion is taken from Hacker's Delight 2nd Edition 7-1.
        for (int i = 0; i < bytes.length; ++i) {
            byte b = bytes[i];
            b = (byte)(((b & 0x55) << 1) | ((b >>> 1) & 0x55));
            b = (byte)(((b & 0x33) << 2) | ((b >>> 2) & 0x33));
            b = (byte)(((b & 0x0f) << 4) | ((b >>> 4) & 0x0f));
            bytes[i] = b;
        }

        return BitSet.valueOf(bytes);
    }

    private static DefaultCharset readDefaultCharset(ByteArrayInputStream inputStream) throws IOException {
        TableMapEventMetadata.DefaultCharset result = new TableMapEventMetadata.DefaultCharset();
        result.setDefaultCharsetCollation(inputStream.readPackedInteger());
        Map<Integer, Integer> charsetCollations = readIntegerPairs(inputStream);
        if (!charsetCollations.isEmpty()) {
            result.setCharsetCollations(charsetCollations);
        }
        return result;
    }

    private static DefaultCharset readDefaultCharset(BinaryLogEventDataReader eventDataReader) throws IOException {
        TableMapEventMetadata.DefaultCharset result = new TableMapEventMetadata.DefaultCharset();
        result.setDefaultCharsetCollation(eventDataReader.readPackedInteger());
        Map<Integer, Integer> charsetCollations = readIntegerPairs(eventDataReader);
        if (!charsetCollations.isEmpty()) {
            result.setCharsetCollations(charsetCollations);
        }
        return result;
    }

    private static List<Integer> readIntegers(ByteArrayInputStream inputStream) throws IOException {
        List<Integer> result = new ArrayList<Integer>();
        while (inputStream.available() > 0) {
            result.add(inputStream.readPackedInteger());
        }
        return result;
    }

    private static List<Integer> readIntegers(BinaryLogEventDataReader eventDataReader) throws IOException {
        List<Integer> result = new ArrayList<>();
        while (eventDataReader.available() > 0) {
            result.add(eventDataReader.readPackedInteger());
        }
        return result;
    }

    private static List<String> readColumnNames(ByteArrayInputStream inputStream) throws IOException {
        List<String> columnNames = new ArrayList<String>();
        while (inputStream.available() > 0) {
            columnNames.add(inputStream.readLengthEncodedString());
        }
        return columnNames;
    }

    private static List<String> readColumnNames(BinaryLogEventDataReader eventDataReader) throws IOException {
        List<String> columnNames = new ArrayList<>();
        while (eventDataReader.available() > 0) {
            columnNames.add(eventDataReader.readLengthEncodedString());
        }
        return columnNames;
    }

    private static List<String[]> readTypeValues(ByteArrayInputStream inputStream) throws IOException {
        List<String[]> result = new ArrayList<String[]>();
        while (inputStream.available() > 0) {
            List<String> typeValues = new ArrayList<String>();
            int valuesCount = inputStream.readPackedInteger();
            for (int i = 0; i < valuesCount; ++i) {
                typeValues.add(inputStream.readLengthEncodedString());
            }
            result.add(typeValues.toArray(new String[typeValues.size()]));
        }
        return result;
    }

    private static List<String[]> readTypeValues(BinaryLogEventDataReader eventDataReader) throws IOException {
        List<String[]> result = new ArrayList<>();
        while (eventDataReader.available() > 0) {
            int valuesCount = eventDataReader.readPackedInteger();
            String[] typeValues = new String[valuesCount];
            for (int i = 0; i < valuesCount; ++i) {
                typeValues[i] = eventDataReader.readLengthEncodedString();
            }
            result.add(typeValues);
        }
        return result;
    }

    private static Map<Integer, Integer> readIntegerPairs(ByteArrayInputStream inputStream) throws IOException {
        Map<Integer, Integer> result = new LinkedHashMap<Integer, Integer>();
        while (inputStream.available() > 0) {
            int columnIndex = inputStream.readPackedInteger();
            int columnCharset = inputStream.readPackedInteger();
            result.put(columnIndex, columnCharset);
        }
        return result;
    }

    private static Map<Integer, Integer> readIntegerPairs(BinaryLogEventDataReader eventDataReader) throws IOException {
        Map<Integer, Integer> result = new LinkedHashMap<>();
        while (eventDataReader.available() > 0) {
            int columnIndex = eventDataReader.readPackedInteger();
            int columnCharset = eventDataReader.readPackedInteger();
            result.put(columnIndex, columnCharset);
        }
        return result;
    }

    private enum MetadataFieldType {
        SIGNEDNESS(1),                      // Signedness of numeric colums
        DEFAULT_CHARSET(2),                 // Charsets of character columns
        COLUMN_CHARSET(3),                  // Charsets of character columns
        COLUMN_NAME(4),                     // Names of columns
        SET_STR_VALUE(5),                   // The string values of SET columns
        ENUM_STR_VALUE(6),                  // The string values is ENUM columns
        GEOMETRY_TYPE(7),                   // The real type of geometry columns
        SIMPLE_PRIMARY_KEY(8),              // The primary key without any prefix
        PRIMARY_KEY_WITH_PREFIX(9),         // The primary key with some prefix
        ENUM_AND_SET_DEFAULT_CHARSET(10),   // Charsets of ENUM and SET columns
        ENUM_AND_SET_COLUMN_CHARSET(11),    // Charsets of ENUM and SET columns
        VISIBILITY(12),                     // Column visibility (8.0.23 and newer)
        UNKNOWN_METADATA_FIELD_TYPE(
            128);   // Returned with binlog-row-metadata=FULL from MySQL 8.0 in some cases

        private final int code;

        private MetadataFieldType(int code) {
            this.code = code;
        }

        public int getCode() {return code;}

        private static final MetadataFieldType[] INDEX_BY_CODE;

        static {
            INDEX_BY_CODE = new MetadataFieldType[MetadataFieldType.UNKNOWN_METADATA_FIELD_TYPE.code + 1];
            for (MetadataFieldType fieldType : values()) {
                INDEX_BY_CODE[fieldType.code] = fieldType;
            }
        }

        public static MetadataFieldType byCode(int code) {
            return INDEX_BY_CODE[code];
        }
    }
}

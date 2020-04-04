#pragma once

#include "../utils/serial.h"
#include "../utils/column_type.h"
#include "../utils/key.h"
#include "../network/shared/network.h"

/**
 * Describes the location of one column and its type in the distributed value store
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ColumnDescription: public Object {
    public:

        /** The location of the data for the column */
        Key** keys = nullptr;

        /** The number of chunks in the column */
        uint64_t chunks = 0;

        /** The total number of elements in the column */
        uint64_t totalLength = 0;

        /** The type of the column */
        ColumnType type = (ColumnType)'\0';

        /** Default constructor */
        ColumnDescription(Key **keys, uint64_t chunks, uint64_t totalLength, ColumnType type) : keys(keys),
                                                                                                chunks(chunks),
                                                                                                totalLength(totalLength),
                                                                                                type(type) {}

        /** Constructor for deserialization */
        ColumnDescription() {}

        ~ColumnDescription() {
            for (size_t i = 0; i < chunks; i++) {
                delete keys[i];
            }

            delete[] keys;
        }

        /** Writes this description out to a buffer */
        void serialize(Serializer &serializer) {
            serializer.write(chunks);
            serializer.write(totalLength);
            serializer.write((uint8_t)type);
            for (size_t i = 0; i < chunks; i++) {
                serializer.write(*keys[i]);
            }
        }

        /** Reads a description from a buffer */
        void deserialize(Deserializer &deserializer) {
            chunks = deserializer.read_uint64();
            totalLength = deserializer.read_uint64();
            type = (ColumnType)deserializer.read_uint8();

            keys = new Key*[chunks];
            for (size_t i = 0; i < chunks; i++) {
                keys[i] = deserializer.read_key();
            }
        }

};

/**
 * Contains the description of a dataframe, where to find all of the columns and what the schema is
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class DataframeDescription: public Object, public Codable {
    public:

        /** The schema for the dataframe in string format. Owns this string */
        String* schema;

        /** The number of columns in the _columns array */
        uint64_t numColumns;

        /** Description of all of the columns. Owns this array. */
        ColumnDescription** columns;

        /** Default constructor */
        DataframeDescription(String *schema, uint64_t numColumns, ColumnDescription **columns) : schema(schema),
                                                                                                numColumns(numColumns),
                                                                                                columns(columns) {}
        /** Constructor for deserialization */
        DataframeDescription() {}

        ~DataframeDescription() {
            delete schema;

            for (uint64_t i = 0; i < numColumns; i++) {
                delete columns[i];
            }

            delete[] columns;
        }

        /** Writes the description out to a buffer */
        void serialize(Serializer &serializer) {
            serializer.write(schema);
            serializer.write(numColumns);

            for (uint64_t i = 0; i < numColumns; i++) {
                columns[i]->serialize(serializer);
            }
        }

        /** Reads the description from a buffer */
        void deserialize(Deserializer &deserializer) {
            schema = deserializer.read_string();
            numColumns = deserializer.read_uint64();

            columns = new ColumnDescription*[numColumns];
            for (uint64_t i = 0; i < numColumns; i++) {
                columns[i] = new ColumnDescription();
                columns[i]->deserialize(deserializer);
            }
        }

};
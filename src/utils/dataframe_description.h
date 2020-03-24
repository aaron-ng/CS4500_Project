#pragma once

#include "../network/serial.h"
#include "../dataframe/column_type.h"
#include "key.h"
#include "../network/network.h"

/**
 * Describes the location of one column and its type in the distributed value store
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ColumnDescription: public Codable {
    public:

        /** The location of the data for the column */
        Key* location = nullptr;

        /** The type of the column */
        ColumnType type = (ColumnType)'\0';

        /** Default constructor */
        ColumnDescription(const Key &location, ColumnType type) : location(new Key(location.getName(), location.getNode())), type(type) {}

        /** Constructor for deserialization */
        ColumnDescription() {}

        ~ColumnDescription() {
            delete location;
        }

        /** Writes this description out to a buffer */
        void serialize(Serializer &serializer) {
            serializer.write(location->_name);
            serializer.write((uint32_t)location->getNode());
            serializer.write((uint8_t)type);
        }

        /** Reads a description from a buffer */
        void deserialize(Deserializer &deserializer) {
            String* name = deserializer.read_string();
            size_t node = deserializer.read_uint32();
            location = new Key(name->c_str(), node);
            delete name;

            type = (ColumnType)deserializer.read_uint8();
        }

};

/**
 * Contains the description of a dataframe, where to find all of the columns and what the schema is
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class DataframeDescription: public Codable {
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
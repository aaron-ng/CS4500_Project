#pragma once

#include <stdio.h>
#include <algorithm>

#include "lineparser.h"
#include "../values/valuefactory.h"
#include "../../dataframe/utils.h"

/** A class that will build a Sor schema from a stream */
class DataAdapter {
    private:

        /** Object used to tokenize the SoR lines */
        const LineParser lineParser;

        /** Object used to build a schema once the longest line is known */
        const ValueFactory valueFactory;

    public:

        DataAdapter() : lineParser(), valueFactory() {}

        /**
         * Determines the schema from a file
         * @param file The file to determine the schema from
         * @return The schema for the file
         */
        Schema determineSchema(FILE* file) {
            std::vector<ColumnType > types;
            rewind(file);

            char* line = nullptr;
            size_t length = 0;
            size_t lines = 0;

            while ((getline(&line, &length, file)) != EOF) {
                if (lines < 500) {
                    std::vector<std::string> tokenizedLine = lineParser.parseTokens(line, types.size());
                    std::vector<ColumnType> lineTypes = valueFactory.getSchema(tokenizedLine);
                    for (size_t i = 0; i < lineTypes.size(); i++) {
                        if (i < types.size()) { types[i] = max(types[i], lineTypes[i]); }
                        else { types.push_back(lineTypes[i]); }
                    }
                }

                lines++;
            }

            if (line) { free(line); }

            std::string schemaStr;
            for (ColumnType type : types) { schemaStr += (char)type; }
            Schema schema(schemaStr.c_str());

            rewind(file);
            return schema;
        }

        /**
         * Reads a single row from the SOR file
         * @param row The row to write the values to
         * @param file The file to read from
         * @param schema The schema of the row
         * @return true if there is more data to read, false otherwise
         */
        bool read(Row& row, FILE* file, Schema& schema) {
            char* line = nullptr;
            size_t length = 0;
            size_t read = 0;

            if ((read = getline(&line, &length, file)) && read != EOF) {
                std::vector<std::string> tokens = lineParser.parseTokens(line, schema.width());

                valueFactory.populateRow(schema, tokens, row);
                return true;
            } else { return false; }
        }

};
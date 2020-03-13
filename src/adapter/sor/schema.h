#pragma once

#include <stdio.h>
#include <algorithm>

#include "../../dataframe/modified_dataframe.h"
#include "lineparser.h"
#include "valuefactory.h"

/** A class that will build a Sor schema from a stream */
class SchemaBuilder {
    private:

        /** Object used to tokenize the SoR lines */
        const LineParser lineParser;

        /** Object used to build a schema once the longest line is known */
        const ValueFactory valueFactory;

    public:

        SchemaBuilder() : lineParser(), valueFactory() {}

        /**
         * Builds a dataframe from a file. This will scan over the first 500 lines, or the entire file, whichever is smaller.
         * The longest line will be parsed, and the resulting schema will be returned. If the longest line is malformed,
         * the schema is considered malformed and an exception is thrown.
         * @param file The file to read from
         * @return The resulting dataframe
         */
        DataFrame* build(FILE* file) const {
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
                        if (i < types.size()) { types[i] = std::max(types[i], lineTypes[i]); }
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
            return populate(file, schema);
        }

    private:

        /**
         * Builds a dataframe using a schema. Each line in the file will correspond to a row in the dataframe
         *
         * @param file The file to read from.
         * @param columns The columns to populate. This is interpreted as the schema of the file
         * @param readEnd The byte offset at which to stop reading. Any line that intersects that byte offset
         *                will be ignored. -1 will read the entire file.
         */
        DataFrame* populate(FILE* file, Schema& schema, long int readEnd = -1) const {
            size_t startingPos = ftell(file);
            size_t currentPos = startingPos;

            char* line = nullptr;
            size_t length = 0;
            size_t read = 0;

            Row row(schema);
            DataFrame* dataFrame = new DataFrame(schema);

            while ((read = getline(&line, &length, file)) && read != EOF && (readEnd == -1 || currentPos + read <= readEnd)) {
                std::vector<std::string> tokens = lineParser.parseTokens(line, schema.length());

                valueFactory.populateRow(schema, tokens, row);
                dataFrame->add_row(row);

                currentPos += read;
            }

            if (line) { free(line); }
            fseek(file, startingPos, SEEK_SET);

            return dataFrame;
        }

};
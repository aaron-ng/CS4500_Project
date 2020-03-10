#pragma once

#include <algorithm>

#include "lineparser.h"
#include "schema.h"

typedef std::vector<Column*> SOR;

/** A class that will build a Sor schema from a stream */
class Schema {
    private:

        /** Object used to tokenize the SoR lines */
        const LineParser lineParser;

        /** Object used to build a schema once the longest line is known */
        const ValueFactory valueFactory;

    public:

        Schema() : lineParser(), valueFactory() {}

        /**
         * Builds the schema from a file. This will scan over the first 500 lines, or the entire file, whichever is smaller.
         * The longest line will be parsed, and the resulting schema will be returned. If the longest line is malformed,
         * the schema is considered malformed and an exception is thrown.
         * @param file The file to read from
         * @return The resulting schema from the longest line
         */
        SOR build(FILE* file) const {
            std::vector<ValueType> types;
            rewind(file);

            char* line = nullptr;
            size_t length = 0;
            size_t lines = 0;

            while ((getline(&line, &length, file)) != EOF) {
                if (lines < 500) {
                    std::vector<std::string> tokenizedLine = lineParser.parseTokens(line, types.size());
                    std::vector<ValueType> lineTypes = valueFactory.getSchema(tokenizedLine);
                    for (size_t i = 0; i < lineTypes.size(); i++) {
                        if (i < types.size()) { types[i] = std::max(types[i], lineTypes[i]); }
                        else { types.push_back(lineTypes[i]); }
                    }
                }

                lines++;
            }

            if (line) { free(line); }

            // Ensure that there is no column with a NONE type
            for (ValueType type : types) {
                if (type == UNKNOWN) { throw std::runtime_error("Malformed schema"); }
            }

            std::vector<Column*> schema(types.size());
            for (int i = 0; i < types.size(); i++) {
                schema[i] = new Column(types[i], lines);
            }

            rewind(file);
            return schema;
        }

        /**
         * Populates rows in a column from an SOR file. If any row is invalid for some reason, the values
         * that were valid before the error was encountered will be added and everything else on the row will be given
         * empty. If a value does not match the type in the schema, it will be replaced with an empty
         *
         * @param file The file to read from.
         * @param columns The columns to populate. This is interpreted as the schema of the file
         * @param readEnd The byte offset at which to stop reading. Any line that intersects that byte offset
         *                will be ignored. -1 will read the entire file.
         */
        void populate(FILE* file, SOR& columns, long int readEnd = -1) const {
            size_t startingPos = ftell(file);
            size_t currentPos = startingPos;

            char* line = nullptr;
            size_t length = 0;
            size_t read = 0;
            while ((read = getline(&line, &length, file)) && read != EOF && (readEnd == -1 || currentPos + read <= readEnd)) {
                std::vector<std::string> tokens = lineParser.parseTokens(line, columns.size());
                valueFactory.addRow(columns, tokens);
                currentPos += read;
            }

            if (line) { free(line); }
            fseek(file, startingPos, SEEK_SET);
        }

};
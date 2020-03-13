#pragma once

#include <vector>
#include <unordered_map>

#include "../values/valueproducer.h"
#include "../values/valueproducers.h"

/**
 * A class that will build values from strings from an SoR file between delimiters
 */
class ValueFactory {
    private:
        /** The producers used to produce values in order from highest to lowest priority */
        const std::vector<ValueProducer*> producers = { new BoolProducer(), new IntProducer(), new FloatProducer(), new StringProducer() };

        /** The producers in producers based on the type of value they produce */
        std::vector<ValueProducer*> orderedProducers;

        /**
         * Returns the index of the producer that produces the given type in orderedProducers
         * @param type The type of producer to return the index for
         */
        inline size_t indexForColumnType(char type) const {
            switch (type) {
                case STRING: return 0;
                case INT: return 1;
                case FLOAT: return 2;
                case BOOL: return 3;
                default: return -1;
            }
        }

    public:

        ValueFactory() {
            orderedProducers.resize(producers.size());
            for (ValueProducer* producer : producers) { orderedProducers[indexForColumnType(producer->producedType())] = producer; }
        }

        ~ValueFactory() { for (ValueProducer* producer : producers) { delete producer; } }

        /**
         * Builds the schema from the given line SoR tokens. These tokens should have no leading or trailing whitespace
         * and not have < or >.
         * @param tokens The tokens to determine the schema from
         * @return The schema for the given row
         */
        std::vector<ColumnType> getSchema(const std::vector<std::string>& tokens) const {
            std::vector<ColumnType> outSchema;

            for (auto i = tokens.begin(); i != tokens.end(); i++) {
                bool added = false;
                for (ValueProducer* producer : producers) {
                    if (producer->canProduce(*i)) {
                        outSchema.push_back(producer->producedType());
                        added = true;
                        break;
                    }
                }

                if (!added) { outSchema.push_back(BOOL); }
            }

            return outSchema;
        }

        /**
         * Populates the row using the value producers. The row is assumed to match the schema
         *
         * If there are no tokens passed in, the entire row is skipped.
         *
         * @param schema The schema to use to populate the row
         * @param tokens The tokens to parse using the columns as the schema.
         * @param row The row to write the data to
         */
        void populateRow(Schema& schema, const std::vector<std::string>& tokens, Row& row) const {
            if (tokens.empty()) { return; }

            for (int i = 0; i < schema.width(); i++) {
                char type = schema.col_type(i);
                if (i < tokens.size()) {
                    orderedProducers[indexForColumnType(type)]->produce(tokens[i], row, i);
                } else { /* EMPTY */ }
            }
        }
};
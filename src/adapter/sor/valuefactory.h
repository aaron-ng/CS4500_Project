#pragma once

#include <vector>
#include <unordered_map>

#include "../table/column.h"
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

    public:

        ValueFactory() {
            orderedProducers.resize(producers.size());
            for (ValueProducer* producer : producers) { orderedProducers[producer->producedType()] = producer; }
        }

        ~ValueFactory() { for (ValueProducer* producer : producers) { delete producer; } }

        /**
         * Builds the schema from the given line SoR tokens. These tokens should have no leading or trailing whitespace
         * and not have < or >.
         * @param tokens The tokens to determine the schema from
         * @return The schema for the given row
         */
        std::vector<ValueType> getSchema(const std::vector<std::string>& tokens) const {
            std::vector<ValueType> outSchema;

            for (auto i = tokens.begin(); i != tokens.end(); i++) {
                bool added = false;
                for (ValueProducer* producer : producers) {
                    if (producer->canProduce(*i)) {
                        outSchema.push_back(producer->producedType());
                        added = true;
                        break;
                    }
                }

                if (!added) { outSchema.push_back(UNKNOWN); }
            }

            return outSchema;
        }

        /**
         * Adds a row to the columns using a collection of given SoR tokens These tokens should have no leading or trailing whitespace
         * and not have < or >. If the number of tokens is less than the number of columns, the columns without corresponding tokens
         * will be populated with empties.
         *
         * If there are no tokens passed in, the entire row is skipped.
         *
         * @param columns The columns to append to
         * @param tokens The tokens to parse using the columns as the schema.
         */
        void addRow(const std::vector<Column*>& columns, const std::vector<std::string>& tokens) const {
            if (tokens.empty()) { return; }

            for (int i = 0; i < columns.size(); i++) {
                Column* column = columns[i];
                if (i < tokens.size()) {
                    Value* producedValue = orderedProducers[column->valueType]->produce(tokens[i]);
                    column->appendRow(producedValue ? producedValue : new EmptyValue());
                } else { column->appendRow(new EmptyValue()); }
            }
        }
};
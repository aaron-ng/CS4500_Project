#pragma once

#include "../../dataframe/column_type.h"

/**
 * A factory that produces a value
 */
class ValueProducer {
    public:

        virtual ~ValueProducer() {}

        /**
         * Attempts to produce a value from the given string. If a value cannot be produce the behavior is undefined.
         * @param str The string to parse a value from
         * @param row The row to add the value to
         * @param idx The index in the row to add the value in
         * @return The resulting value or nullptr if a value could not be parsed
         */
        virtual void produce(const std::string& str, Row& row, size_t idx) = 0;

        /**
         * Returns true if this value producer can produce a value from the string.
         * @param str The string to determine if a value can be produced.
         * @return true if a value can be produced, false otherwise.
         */
        virtual bool canProduce(const std::string& str) = 0;

        /** Returns the value type that this producer produces */
        virtual ColumnType producedType() const = 0;
};
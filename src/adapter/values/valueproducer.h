#pragma once

#include "value.h"
#include "valuetypes.h"

/**
 * A factory that produces a value
 */
class ValueProducer {
    public:

        virtual ~ValueProducer() {}

        /**
         * Attempts to produce a value from the given string. If a value cannot be produced, return nullptr.
         * @param str The string to parse a value from
         * @return The resulting value or nullptr if a value could not be parsed
         */
        virtual Value* produce(const std::string& str) = 0;

        /**
         * Returns true if this value producer can produce a value from the string.
         * @param str The string to determine if a value can be produced.
         * @return true if a value can be produced, false otherwise.
         */
        virtual bool canProduce(const std::string& str) = 0;

        /** Returns the value type that this producer produces */
        virtual ValueType producedType() const = 0;
};
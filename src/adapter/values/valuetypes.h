#pragma once

#include <cstdint>

/**
 * The different types of values that can be found inside a SoR file. The value of each case
 * is the priority in the schema.
 */
enum ValueType: int8_t {
    UNKNOWN = -1,
    BOOL = 0,
    INT = 1,
    FLOAT = 2,
    STRING = 3
};

/**
 * Returns a user-readable name of the value type
 * @param type The type to name
 * @return The name of the given type
 */
std::string nameOfValueType(ValueType type) {
    switch (type) {
        case STRING: return "STRING";
        case INT: return "INT";
        case FLOAT: return "FLOAT";
        case BOOL: return "BOOL";
        default: return "UNKNOWN";
    }
}
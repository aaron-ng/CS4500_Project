#pragma once

#include <cstdint>

#include "../../dataframe/column_type.h"

/**
 * Returns a user-readable name of the value type
 * @param type The type to name
 * @return The name of the given type
 */
std::string nameOfValueType(ColumnType type) {
    switch (type) {
        case STRING: return "STRING";
        case INT: return "INT";
        case FLOAT: return "FLOAT";
        case BOOL: return "BOOL";
        default: return "UNKNOWN";
    }
}
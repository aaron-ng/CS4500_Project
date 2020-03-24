#pragma once

/**
 * The various types of columns
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
enum ColumnType: unsigned char {
    INT = 'I',
    FLOAT = 'F',
    BOOL = 'B',
    STRING = 'S'
};

/**
 * Returns the priority of the given column type The priority from lowest to highest
 * is BOOL, INT, FLOAT, STRING
 */
uint8_t priority(ColumnType a) {
    switch (a) {
        case BOOL: return 0;
        case INT: return 1;
        case FLOAT: return 2;
        case STRING: return 3;
    }
}

/**
 * Returns the column type out of the two that has a higher priority. The priority from lowest to highest
 * is BOOL, INT, FLOAT, STRING
 */
ColumnType max(ColumnType a, ColumnType b) {
    return priority(a) > priority(b) ? a : b;
}


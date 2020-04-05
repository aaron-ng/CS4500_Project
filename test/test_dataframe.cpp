#include <gtest/gtest.h>

#include "utils.h"
#include "../src/utils/datastructures/element_column.h"
#include "../src/utils/column_type.h"
#include "../src/dataframe/dataframe.h"
#include "../src/dataframe/columns/chunked_column.h"


/* Start element column tests                                      */
/*-----------------------------------------------------------------*/

void testPagingWorks() {
    ElementColumn column;

    size_t pages = 5;
    for (size_t page = 0; page < pages; page++) {
        Element* last = column.grow();

        for (size_t i = 0; i < ElementColumn::_CHUNK_SIZE - 1; i++) {
            Element* next = column.grow();
            GT_TRUE(next == last + 1);
            last = next;
        }
    }

    GT_TRUE(column.size() == ElementColumn::_CHUNK_SIZE * pages);
    GT_TRUE(column._numChunks == pages);

    exit(0);
}

void testGetWorks() {
    ElementColumn column;

    size_t pages = 5;
    size_t elements = 0;
    for (size_t page = 0; page < pages; page++) {
        column.grow();
        elements++;

        for (size_t i = 0; i < ElementColumn::_CHUNK_SIZE - 1; i++) {
            column.grow();
            elements++;

            GT_TRUE( column.get(elements - 1) == column.get(elements - 2) + 1);
        }
    }

    exit(0);
}

TEST(W1, testPagingWorks) { ASSERT_EXIT_ZERO(testPagingWorks) }
TEST(W1, testGetWorks) { ASSERT_EXIT_ZERO(testGetWorks) }

/* End element column tests                                        */
/*-----------------------------------------------------------------*/

/* Start schema tests                                              */
/*-----------------------------------------------------------------*/

#define COLUMN_LEN 6
const char columnTypes[] = {ColumnType::INT, ColumnType::STRING, ColumnType::STRING, ColumnType::BOOL, ColumnType::DOUBLE, ColumnType::INT, '\0'};
const char* columnNames[] = {"INT_COL", "STRING_COL", "STRING_COL_1", "BOOL_COL", "DOUBLE_COL", "INT_COL_1"};

void testSchemaConstructorWorks() {
    Schema schema(columnTypes);

    GT_TRUE(schema.width() == COLUMN_LEN);
    for (size_t i = 0; i < COLUMN_LEN; i++) {
        GT_TRUE(schema.col_type(i) == columnTypes[i]);
    }

    exit(0);
}

void testSchemaAddColumnWorks() {
    Schema schema("");

    for (size_t i = 0; i < COLUMN_LEN; i++) {
        String name(columnNames[i]);
        schema.add_column(columnTypes[i], &name);
    }

    GT_TRUE(schema.width() == COLUMN_LEN);
    for (size_t i = 0; i < COLUMN_LEN; i++) {
        GT_TRUE(schema.col_type(i) == columnTypes[i]);
        GT_TRUE(!strcmp(schema.col_name(i)->c_str(), columnNames[i]));
    }

    exit(0);
}

void testColumnNameIndexWorks() {
    Schema schema(columnTypes);

    for (size_t i = 0; i < COLUMN_LEN; i++) {
        String name(columnNames[i]);
        schema.add_column(columnTypes[i], &name);
    }

    GT_TRUE(schema.width() == COLUMN_LEN * 2);
    GT_TRUE(schema.col_idx(nullptr) == -1);
    for (size_t i = 0; i < COLUMN_LEN; i++) {
        GT_TRUE(schema.col_idx(columnNames[i]) == COLUMN_LEN + i);
    }

    exit(0);
}

TEST(W1, testSchemaConstructorWorks) { ASSERT_EXIT_ZERO(testSchemaConstructorWorks) }
TEST(W1, testSchemaAddColumnWorks) { ASSERT_EXIT_ZERO(testSchemaAddColumnWorks) }
TEST(W1, testColumnNameIndexWorks) { ASSERT_EXIT_ZERO(testColumnNameIndexWorks) }

/* End schema tests                                                */
/*-----------------------------------------------------------------*/

/* Start IntColumn tests                                           */
/*-----------------------------------------------------------------*/

#define INT_VALUES 5
const int intValues[] {-1231238, 84393389, 0, 123, -9696};

void testIntTypeWorks() {
    GT_TRUE(FullIntColumn().get_type() == INT);
    exit(0);
}

void testIntVarArgsConstructorWorks() {
    FullIntColumn column(INT_VALUES, intValues[0], intValues[1], intValues[2], intValues[3], intValues[4]);

    GT_TRUE(column.size() == INT_VALUES);
    for (size_t i = 0; i < INT_VALUES; i++) {
        GT_TRUE(column.get(i) == intValues[i]);
    }

    exit(0);
}

void testIntPushBackWorks() {
    FullIntColumn column(INT_VALUES, intValues[0], intValues[1], intValues[2], intValues[3], intValues[4]);
    for (size_t i = 0; i < INT_VALUES; i++) {
        column.push_back(intValues[i]);
    }

    GT_TRUE(column.size() == INT_VALUES * 2);
    for (size_t i = 0; i < INT_VALUES * 2; i++) {
        GT_TRUE(column.get(i) == intValues[i % INT_VALUES]);
    }

    exit(0);
}

void testIntSetWorks() {
    FullIntColumn column(INT_VALUES, intValues[0], intValues[1], intValues[2], intValues[3], intValues[4]);
    for (size_t i = 0; i < INT_VALUES; i++) {
        column.set(i, intValues[INT_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == INT_VALUES);
    for (size_t i = 0; i < INT_VALUES; i++) {
        GT_TRUE(column.get(i) == intValues[INT_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W1, testIntTypeWorks) { ASSERT_EXIT_ZERO(testIntTypeWorks) }
TEST(W1, testIntVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testIntVarArgsConstructorWorks) }
TEST(W1, testIntPushBackWorks) { ASSERT_EXIT_ZERO(testIntPushBackWorks) }
TEST(W1, testIntSetWorks) { ASSERT_EXIT_ZERO(testIntSetWorks) }

/* End IntColumn tests                                             */
/*-----------------------------------------------------------------*/

/* Start DoubleColumn tests                                         */
/*-----------------------------------------------------------------*/

#define DOUBLE_VALUES 5
const double doubleValues[] {-1238.12323, 8439.3389, 0.0, 123.55555552, -9696.0010010};

void testDoubleTypeWorks() {
    GT_TRUE(FullDoubleColumn().get_type() == DOUBLE);
    exit(0);
}

void testDoubleVarArgsConstructorWorks() {
    FullDoubleColumn column(DOUBLE_VALUES, doubleValues[0], doubleValues[1], doubleValues[2], doubleValues[3], doubleValues[4]);

    GT_TRUE(column.size() == DOUBLE_VALUES);
    for (size_t i = 0; i < DOUBLE_VALUES; i++) {
        GT_TRUE(column.get(i) == doubleValues[i]);
    }

    exit(0);
}

void testDoublePushBackWorks() {
    FullDoubleColumn column(DOUBLE_VALUES, doubleValues[0], doubleValues[1], doubleValues[2], doubleValues[3], doubleValues[4]);
    for (size_t i = 0; i < DOUBLE_VALUES; i++) {
        column.push_back(doubleValues[i]);
    }

    GT_TRUE(column.size() == DOUBLE_VALUES * 2);
    for (size_t i = 0; i < DOUBLE_VALUES * 2; i++) {
        GT_TRUE(column.get(i) == doubleValues[i % DOUBLE_VALUES]);
    }

    exit(0);
}

void testDoubleSetWorks() {
    FullDoubleColumn column(DOUBLE_VALUES, doubleValues[0], doubleValues[1], doubleValues[2], doubleValues[3], doubleValues[4]);
    for (size_t i = 0; i < DOUBLE_VALUES; i++) {
        column.set(i, doubleValues[DOUBLE_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == DOUBLE_VALUES);
    for (size_t i = 0; i < DOUBLE_VALUES; i++) {
        GT_TRUE(column.get(i) == doubleValues[DOUBLE_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W1, testDoubleTypeWorks) { ASSERT_EXIT_ZERO(testDoubleTypeWorks) }
TEST(W1, testDoubleVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testDoubleVarArgsConstructorWorks) }
TEST(W1, testDoublePushBackWorks) { ASSERT_EXIT_ZERO(testDoublePushBackWorks) }
TEST(W1, testDoubleSetWorks) { ASSERT_EXIT_ZERO(testDoubleSetWorks) }

/* End DoubleColumn tests                                           */
/*-----------------------------------------------------------------*/

/* Start BoolColumn tests                                          */
/*-----------------------------------------------------------------*/

#define BOOL_VALUES 5
const bool boolValues[] {true, true, false, true, false};

void testBoolTypeWorks() {
    GT_TRUE(FullBoolColumn().get_type() == BOOL);
    exit(0);
}

void testBoolVarArgsConstructorWorks() {
    FullBoolColumn column(BOOL_VALUES, boolValues[0], boolValues[1], boolValues[2], boolValues[3], boolValues[4]);

    GT_TRUE(column.size() == BOOL_VALUES);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        GT_TRUE(column.get(i) == boolValues[i]);
    }

    exit(0);
}

void testBoolPushBackWorks() {
    FullBoolColumn column(BOOL_VALUES, boolValues[0], boolValues[1], boolValues[2], boolValues[3], boolValues[4]);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        column.push_back(boolValues[i]);
    }

    GT_TRUE(column.size() == BOOL_VALUES * 2);
    for (size_t i = 0; i < BOOL_VALUES * 2; i++) {
        GT_TRUE(column.get(i) == boolValues[i % BOOL_VALUES]);
    }

    exit(0);
}

void testBoolSetWorks() {
    FullBoolColumn column(BOOL_VALUES, boolValues[0], boolValues[1], boolValues[2], boolValues[3], boolValues[4]);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        column.set(i, boolValues[BOOL_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == BOOL_VALUES);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        GT_TRUE(column.get(i) == boolValues[BOOL_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W1, testBoolTypeWorks) { ASSERT_EXIT_ZERO(testBoolTypeWorks) }
TEST(W1, testBoolVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testBoolVarArgsConstructorWorks) }
TEST(W1, testBoolPushBackWorks) { ASSERT_EXIT_ZERO(testBoolPushBackWorks) }
TEST(W1, testBoolSetWorks) { ASSERT_EXIT_ZERO(testBoolSetWorks) }

/* End BoolColumn tests                                            */
/*-----------------------------------------------------------------*/

/* Start StringColumn tests                                        */
/*-----------------------------------------------------------------*/

#define STRING_VALUES 5
String* stringValues[STRING_VALUES] = {new String("hello"), new String("bye"), new String("wat"), new String("nooo"), new String("yes")};

void testStringTypeWorks() {
    GT_TRUE(FullStringColumn().get_type() == STRING);
    exit(0);
}

void testStringVarArgsConstructorWorks() {
    FullStringColumn column(STRING_VALUES, stringValues[0], stringValues[1], stringValues[2], stringValues[3], stringValues[4]);

    GT_TRUE(column.size() == STRING_VALUES);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        GT_TRUE(column.get(i) == stringValues[i]);
    }

    exit(0);
}

void testStringPushBackWorks() {
    FullStringColumn column(STRING_VALUES, stringValues[0], stringValues[1], stringValues[2], stringValues[3], stringValues[4]);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        column.push_back(stringValues[i]);
    }

    GT_TRUE(column.size() == STRING_VALUES * 2);
    for (size_t i = 0; i < STRING_VALUES * 2; i++) {
        GT_TRUE(column.get(i) == stringValues[i % STRING_VALUES]);
    }

    exit(0);
}

void testStringSetWorks() {
    FullStringColumn column(STRING_VALUES, stringValues[0], stringValues[1], stringValues[2], stringValues[3], stringValues[4]);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        column.set(i, stringValues[STRING_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == STRING_VALUES);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        GT_TRUE(column.get(i) == stringValues[STRING_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W1, testStringTypeWorks) { ASSERT_EXIT_ZERO(testStringTypeWorks) }
TEST(W1, testStringVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testStringVarArgsConstructorWorks) }
TEST(W1, testStringPushBackWorks) { ASSERT_EXIT_ZERO(testStringPushBackWorks) }
TEST(W1, testStringSetWorks) { ASSERT_EXIT_ZERO(testStringSetWorks) }

/* End StringColumn tests                                          */
/*-----------------------------------------------------------------*/

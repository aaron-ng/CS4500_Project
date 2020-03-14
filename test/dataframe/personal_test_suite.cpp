#include <gtest/gtest.h>

#include "../../src/element_column.h"
#include "../../src/dataframe/column_type.h"
#include "../../src/dataframe/modified_dataframe.h"

#define GT_TRUE(a)   ASSERT_EQ((a),true)
#define GT_FALSE(a)  ASSERT_EQ((a),false)
#define GT_EQUALS(a, b)   ASSERT_EQ(a, b)
#define ASSERT_EXIT_ZERO(a)  \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

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
const char columnTypes[] = {ColumnType::INT, ColumnType::STRING, ColumnType::STRING, ColumnType::BOOL, ColumnType::FLOAT, ColumnType::INT, '\0'};
const char* columnNames[] = {"INT_COL", "STRING_COL", "STRING_COL_1", "BOOL_COL", "FLOAT_COL", "INT_COL_1"};

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

void testSchemaAddRowWorks() {
    Schema schema(columnTypes);

    String name0("ROW_NAME");
    String name1("ROW_NAME_1");
    schema.add_row(nullptr);
    schema.add_row(&name0);
    schema.add_row(&name1);

    GT_TRUE(schema.width() == COLUMN_LEN);
    GT_TRUE(schema.length() == 3);

    GT_TRUE(schema.row_name(0) == nullptr);
    GT_TRUE(schema.row_name(1)->equals(&name0));
    GT_TRUE(schema.row_name(2)->equals(&name1));

    exit(0);
}

void testSchemaFindRowNameWorks() {
    Schema schema(columnTypes);

    String name0("ROW_NAME");
    String name1("ROW_NAME_1");
    schema.add_row(nullptr);
    schema.add_row(&name0);
    schema.add_row(&name1);

    GT_TRUE(schema.width() == COLUMN_LEN);
    GT_TRUE(schema.length() == 3);

    GT_TRUE(schema.row_idx(nullptr) == -1);
    GT_TRUE(schema.row_idx(name0.c_str()) == 1);
    GT_TRUE(schema.row_idx(name1.c_str()) == 2);

    exit(0);
}

TEST(W2, testSchemaConstructorWorks) { ASSERT_EXIT_ZERO(testSchemaConstructorWorks) }
TEST(W2, testSchemaAddColumnWorks) { ASSERT_EXIT_ZERO(testSchemaAddColumnWorks) }
TEST(W2, testColumnNameIndexWorks) { ASSERT_EXIT_ZERO(testColumnNameIndexWorks) }
TEST(W2, testSchemaAddRowWorks) { ASSERT_EXIT_ZERO(testSchemaAddRowWorks) }
TEST(W2, testSchemaFindRowNameWorks) { ASSERT_EXIT_ZERO(testSchemaFindRowNameWorks) }

/* End schema tests                                                */
/*-----------------------------------------------------------------*/

/* Start IntColumn tests                                           */
/*-----------------------------------------------------------------*/

#define INT_VALUES 5
const int intValues[] {-1231238, 84393389, 0, 123, -9696};

void testIntTypeWorks() {
    GT_TRUE(IntColumn().get_type() == INT);
    exit(0);
}

void testIntVarArgsConstructorWorks() {
    IntColumn column(INT_VALUES, intValues[0], intValues[1], intValues[2], intValues[3], intValues[4]);

    GT_TRUE(column.size() == INT_VALUES);
    for (size_t i = 0; i < INT_VALUES; i++) {
        GT_TRUE(column.get(i) == intValues[i]);
    }

    exit(0);
}

void testIntPushBackWorks() {
    IntColumn column(INT_VALUES, intValues[0], intValues[1], intValues[2], intValues[3], intValues[4]);
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
    IntColumn column(INT_VALUES, intValues[0], intValues[1], intValues[2], intValues[3], intValues[4]);
    for (size_t i = 0; i < INT_VALUES; i++) {
        column.set(i, intValues[INT_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == INT_VALUES);
    for (size_t i = 0; i < INT_VALUES; i++) {
        GT_TRUE(column.get(i) == intValues[INT_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W3, testIntTypeWorks) { ASSERT_EXIT_ZERO(testIntTypeWorks) }
TEST(W3, testIntVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testIntVarArgsConstructorWorks) }
TEST(W3, testIntPushBackWorks) { ASSERT_EXIT_ZERO(testIntPushBackWorks) }
TEST(W3, testIntSetWorks) { ASSERT_EXIT_ZERO(testIntSetWorks) }

/* End IntColumn tests                                             */
/*-----------------------------------------------------------------*/

/* Start FloatColumn tests                                         */
/*-----------------------------------------------------------------*/

#define FLOAT_VALUES 5
const float floatValues[] {-1238.12323, 8439.3389, 0.0, 123.55555552, -9696.0010010};

void testFloatTypeWorks() {
    GT_TRUE(FloatColumn().get_type() == FLOAT);
    exit(0);
}

void testFloatVarArgsConstructorWorks() {
    FloatColumn column(FLOAT_VALUES, floatValues[0], floatValues[1], floatValues[2], floatValues[3], floatValues[4]);

    GT_TRUE(column.size() == FLOAT_VALUES);
    for (size_t i = 0; i < FLOAT_VALUES; i++) {
        GT_TRUE(column.get(i) == floatValues[i]);
    }

    exit(0);
}

void testFloatPushBackWorks() {
    FloatColumn column(FLOAT_VALUES, floatValues[0], floatValues[1], floatValues[2], floatValues[3], floatValues[4]);
    for (size_t i = 0; i < FLOAT_VALUES; i++) {
        column.push_back(floatValues[i]);
    }

    GT_TRUE(column.size() == FLOAT_VALUES * 2);
    for (size_t i = 0; i < FLOAT_VALUES * 2; i++) {
        GT_TRUE(column.get(i) == floatValues[i % FLOAT_VALUES]);
    }

    exit(0);
}

void testFloatSetWorks() {
    FloatColumn column(FLOAT_VALUES, floatValues[0], floatValues[1], floatValues[2], floatValues[3], floatValues[4]);
    for (size_t i = 0; i < FLOAT_VALUES; i++) {
        column.set(i, floatValues[FLOAT_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == FLOAT_VALUES);
    for (size_t i = 0; i < FLOAT_VALUES; i++) {
        GT_TRUE(column.get(i) == floatValues[FLOAT_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W4, testFloatTypeWorks) { ASSERT_EXIT_ZERO(testFloatTypeWorks) }
TEST(W4, testFloatVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testFloatVarArgsConstructorWorks) }
TEST(W4, testFloatPushBackWorks) { ASSERT_EXIT_ZERO(testFloatPushBackWorks) }
TEST(W4, testFloatSetWorks) { ASSERT_EXIT_ZERO(testFloatSetWorks) }

/* End FloatColumn tests                                           */
/*-----------------------------------------------------------------*/

/* Start BoolColumn tests                                          */
/*-----------------------------------------------------------------*/

#define BOOL_VALUES 5
const bool boolValues[] {true, true, false, true, false};

void testBoolTypeWorks() {
    GT_TRUE(BoolColumn().get_type() == BOOL);
    exit(0);
}

void testBoolVarArgsConstructorWorks() {
    BoolColumn column(BOOL_VALUES, boolValues[0], boolValues[1], boolValues[2], boolValues[3], boolValues[4]);

    GT_TRUE(column.size() == BOOL_VALUES);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        GT_TRUE(column.get(i) == boolValues[i]);
    }

    exit(0);
}

void testBoolPushBackWorks() {
    BoolColumn column(BOOL_VALUES, boolValues[0], boolValues[1], boolValues[2], boolValues[3], boolValues[4]);
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
    BoolColumn column(BOOL_VALUES, boolValues[0], boolValues[1], boolValues[2], boolValues[3], boolValues[4]);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        column.set(i, boolValues[BOOL_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == BOOL_VALUES);
    for (size_t i = 0; i < BOOL_VALUES; i++) {
        GT_TRUE(column.get(i) == boolValues[BOOL_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W5, testBoolTypeWorks) { ASSERT_EXIT_ZERO(testBoolTypeWorks) }
TEST(W5, testBoolVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testBoolVarArgsConstructorWorks) }
TEST(W5, testBoolPushBackWorks) { ASSERT_EXIT_ZERO(testBoolPushBackWorks) }
TEST(W5, testBoolSetWorks) { ASSERT_EXIT_ZERO(testBoolSetWorks) }

/* End BoolColumn tests                                            */
/*-----------------------------------------------------------------*/

/* Start StringColumn tests                                        */
/*-----------------------------------------------------------------*/

#define STRING_VALUES 5
String* stringValues[STRING_VALUES] = {new String("hello"), new String("bye"), new String("wat"), new String("nooo"), new String("yes")};

void testStringTypeWorks() {
    GT_TRUE(StringColumn().get_type() == STRING);
    exit(0);
}

void testStringVarArgsConstructorWorks() {
    StringColumn column(STRING_VALUES, stringValues[0], stringValues[1], stringValues[2], stringValues[3], stringValues[4]);

    GT_TRUE(column.size() == STRING_VALUES);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        GT_TRUE(column.get(i) == stringValues[i]);
    }

    exit(0);
}

void testStringPushBackWorks() {
    StringColumn column(STRING_VALUES, stringValues[0], stringValues[1], stringValues[2], stringValues[3], stringValues[4]);
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
    StringColumn column(STRING_VALUES, stringValues[0], stringValues[1], stringValues[2], stringValues[3], stringValues[4]);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        column.set(i, stringValues[STRING_VALUES - i - 1]);
    }

    GT_TRUE(column.size() == STRING_VALUES);
    for (size_t i = 0; i < STRING_VALUES; i++) {
        GT_TRUE(column.get(i) == stringValues[STRING_VALUES - i - 1]);
    }

    exit(0);
}

TEST(W6, testStringTypeWorks) { ASSERT_EXIT_ZERO(testStringTypeWorks) }
TEST(W6, testStringVarArgsConstructorWorks) { ASSERT_EXIT_ZERO(testStringVarArgsConstructorWorks) }
TEST(W6, testStringPushBackWorks) { ASSERT_EXIT_ZERO(testStringPushBackWorks) }
TEST(W6, testStringSetWorks) { ASSERT_EXIT_ZERO(testStringSetWorks) }

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
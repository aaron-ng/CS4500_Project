#include <gtest/gtest.h>
#include <math.h>

#include "utils.h"
#include "../src/adapter/values/valueproducers.h"
#include "../src/adapter/values/valuefactory.h"
#include "../src/adapter/sor/lineparser.h"

/* Start value producer tests                                      */
/*-----------------------------------------------------------------*/

StringProducer stringProducer = StringProducer();
DoubleProducer floatProducer = DoubleProducer();
IntProducer intProducer = IntProducer();
BoolProducer boolProducer = BoolProducer();

const char columnTypes[5] = {ColumnType::INT, ColumnType::STRING, ColumnType::BOOL, ColumnType::DOUBLE, '\0'};

void testStringProducer() {
    Schema strSchema(columnTypes);
    Row row(strSchema);

    // Test empty string
    stringProducer.produce("", row, 1);
    GT_TRUE(row.get_string(1) == nullptr);

    // Test string with quotes
    stringProducer.produce("\"test hello\"", row, 1);
    String* s1 = row.get_string(1);
    GT_TRUE(s1 != nullptr);
    GT_TRUE(strcmp(s1->c_str(), "test hello") == 0);

    // Test string with space before and after no quotes
    stringProducer.produce("test", row, 1);
    String* s2 = row.get_string(1);
    GT_TRUE(s2 != nullptr);
    GT_TRUE(strcmp(s2->c_str(), "test") == 0);

    // Test string with spaces inside and no quotes
    stringProducer.produce("te  st", row, 1);
    String* s3 = row.get_string(1);
    GT_TRUE(s3 == nullptr);

    exit(0);
}

void testIsValidNumber() {
    GT_TRUE(isValidNumber("1", false));
    GT_TRUE(isValidNumber("1", true));
    GT_FALSE(isValidNumber("1.0", false));
    GT_TRUE(isValidNumber("1.0", true));
    GT_FALSE(isValidNumber("test123", true));
    GT_TRUE(isValidNumber("+1", false));
    GT_TRUE(isValidNumber("+1", true));
    GT_FALSE(isValidNumber("+1.00", false));
    GT_TRUE(isValidNumber("+1.00", true));
    GT_TRUE(isValidNumber("-1", false));
    GT_TRUE(isValidNumber("-1", true));
    GT_FALSE(isValidNumber("-1.00", false));
    GT_TRUE(isValidNumber("-1.00", true));
    GT_FALSE(isValidNumber("", false));

    exit(0);
}

void testDoubleProducer() {
    Schema strSchema(columnTypes);
    Row row(strSchema);

    // Test incorrect float
    row.set(3, 3.0);
    floatProducer.produce("test12", row, 3);
    GT_FALSE(row.get_double(3.0));

    // Test with float
    floatProducer.produce("1.0", row, 3);
    float f1 = row.get_double(3);
    GT_TRUE(f1);
    GT_TRUE(f1 == 1.0);

    exit(0);
}

void testIntProducer() {
    Schema strSchema(columnTypes);
    Row row(strSchema);

    // Test incorrect int
    row.set(0, 999);
    intProducer.produce("test12", row, 0);
    GT_FALSE(row.get_int(999));

    // Test with int
    intProducer.produce("123", row, 0);
    int i1 = row.get_int(0);
    GT_TRUE(i1 == 123);

    exit(0);
}

void testBoolProducer() {
    Schema strSchema(columnTypes);
    Row row(strSchema);


    // Test with invalid bool
    row.set(2, false);
    boolProducer.produce("test12", row, 2);
    GT_FALSE(row.get_bool(2));

    // Test with string = 0
    boolProducer.produce("0", row, 2);
    GT_TRUE(row.get_bool(2) == 0);

    // Test with string = 1;
    boolProducer.produce("1", row, 2);
    GT_TRUE(row.get_bool(2) == 1);

    exit(0);
}

/* End value producer tests                                        */
/*-----------------------------------------------------------------*/

/* Start value factory tests                                       */
/*-----------------------------------------------------------------*/

ValueFactory valueFactory = ValueFactory();
LineParser lineParser = LineParser();

void testGetSchema() {

    // Test basic schema type
    std::vector<std::string> token1 = lineParser.parseTokens("<1>");
    std::vector<ColumnType> schema1 = valueFactory.getSchema(token1);
    GT_TRUE(schema1.size() == 1);
    GT_TRUE(schema1[0] == BOOL);
    token1.clear();
    schema1.clear();

    // Test schema wth all types
    std::vector<std::string> token2 = lineParser.parseTokens("<1>  <123> <4.55> <StringTest>");
    std::vector<ColumnType> schema2 = valueFactory.getSchema(token2);
    GT_TRUE(schema2.size() == 4);
    GT_TRUE(schema2[0] == BOOL);
    GT_TRUE(schema2[1] == INT);
    GT_TRUE(schema2[2] == DOUBLE);
    GT_TRUE(schema2[3] == STRING);
    token2.clear();
    schema2.clear();

    exit(0);
}

void testPopulateRow() {

    // Adding 1 row
    std::vector<std::string> token1 = lineParser.parseTokens("<1>");
    const char columnTypes1[2] = {ColumnType::BOOL, '\0'};
    Schema schema(columnTypes1);
    Row row(schema);
    valueFactory.populateRow(schema, token1, row);

    GT_TRUE(row.get_bool(0));

    // Add a complex row to the table with missing fields
    const char columnTypes2[6] = {ColumnType::BOOL, ColumnType::INT, ColumnType::STRING, ColumnType::DOUBLE, ColumnType::INT, '\0'};
    Schema schema2(columnTypes2);
    Row row2(schema2);
    std::vector<std::string> token2 = lineParser.parseTokens("<1> <123> <\"Hello World\"> <22.22> <>");

    valueFactory.populateRow(schema2, token2, row2);

    GT_TRUE(row2.get_bool(0) == 1);
    GT_TRUE(row2.get_int(1) == 123);
    GT_TRUE(strcmp(row2.get_string(2)->c_str(), "Hello World") == 0);
    GT_TRUE( fabs(row2.get_double(3) - 22.22) < 0.005);

    exit(0);
}

/* End value factory tests                                         */
/*-----------------------------------------------------------------*/

/* Start line parser tests                                         */
/*-----------------------------------------------------------------*/

void testParseTokens() {
    // Test empty string
    std::vector<std::string> v1 = lineParser.parseTokens("");
    GT_TRUE(v1.size() == 0);
    v1.clear();

    // Test only one delim
    std::vector<std::string> v2 = lineParser.parseTokens("<");
    GT_TRUE(v2.size() == 0);
    v2.clear();

    // Test empty space
    std::vector<std::string> v3 = lineParser.parseTokens("   ");
    GT_TRUE(v3.size() == 0);
    v3.clear();

    // Test delim with nothing inside
    std::vector<std::string> v4 = lineParser.parseTokens("<>");
    GT_TRUE(v4.size() == 1);
    v4.clear();

    // Test delim with empty space inside
    std::vector<std::string> v5 = lineParser.parseTokens("<   >");
    GT_TRUE(v5.size() == 1);
    v5.clear();

    // Test delim with string inside
    std::vector<std::string> v6 = lineParser.parseTokens("<test123>");
    GT_TRUE(v6.size() == 1);
    GT_TRUE(v6[0] == "test123");
    v6.clear();

    // Test delim with int inside
    std::vector<std::string> v7 = lineParser.parseTokens("<123>");
    GT_TRUE(v7.size() == 1);
    GT_TRUE(v7[0] == "123");
    v7.clear();

    // Test with spaces within delim
    std::vector<std::string> v8 = lineParser.parseTokens("<  test123      >");
    GT_TRUE(v8.size() == 1);
    GT_TRUE(v8[0] == "test123");
    v8.clear();

    // Test failure with multiple delims
    std::vector<std::string> v9 = lineParser.parseTokens("<<<>");
    GT_TRUE(v9.size() == 0);
    v9.clear();

    // Test with multiple tokens
    std::vector<std::string> v10 = lineParser.parseTokens("<test123> <55> <\"Hello test\">      <yay>");
    GT_TRUE(v10.size() == 4);
    GT_TRUE(v10[0] == "test123");
    GT_TRUE(v10[1] == "55");
    GT_TRUE(v10[2] == "\"Hello test\"");
    GT_TRUE(v10[3] == "yay");
    v10.clear();

    exit(0);
}


TEST(W6, testStringProducer) { ASSERT_EXIT_ZERO(testStringProducer) }
TEST(W6, testIsValidNumber) { ASSERT_EXIT_ZERO(testIsValidNumber) }
TEST(W6, testDoubleProducer) { ASSERT_EXIT_ZERO(testDoubleProducer) }
TEST(W6, testIntProducer) { ASSERT_EXIT_ZERO(testIntProducer) }
TEST(W6, testBoolProducer) { ASSERT_EXIT_ZERO(testBoolProducer) }
TEST(W6, testGetSchema) { ASSERT_EXIT_ZERO(testGetSchema) }
TEST(W6, testPopulateRow) { ASSERT_EXIT_ZERO(testPopulateRow) }
TEST(W6, testParseTokens) { ASSERT_EXIT_ZERO(testParseTokens) }

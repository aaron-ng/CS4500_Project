#pragma once

#include "../../test_util.h"
#include "../test_template.h"
#include "../../../src/adapter/sor/schemabuilder.h"
#include "../../../src/adapter/values/valuefactory.h"
#include "../../../src/adapter/sor/lineparser.h"
#include "../../../src/adapter/values/valuetypes.h"

class Valuefactory_test : public Test_template {

  public:

    virtual void run() {
      testGetSchema();
      testPopulateRow();
    }

  private:
    ValueFactory valueFactory = ValueFactory();
    LineParser lineParser = LineParser();

    void testGetSchema() {

      // Test basic schema type
      std::vector<std::string> token1 = lineParser.parseTokens("<1>");
      std::vector<ColumnType> schema1 = valueFactory.getSchema(token1);
      t_true(schema1.size() == 1);
      t_true(schema1[0] == BOOL);
      token1.clear();
      schema1.clear();

      // Test schema wth all types
      std::vector<std::string> token2 = lineParser.parseTokens("<1>  <123> <4.55> <StringTest>");
        std::vector<ColumnType> schema2 = valueFactory.getSchema(token2);
      t_true(schema2.size() == 4);
      t_true(schema2[0] == BOOL);
      t_true(schema2[1] == INT);
      t_true(schema2[2] == FLOAT);
      t_true(schema2[3] == STRING);
      token2.clear();
      schema2.clear();
      
      OK("ValueFactory.getSchema");
    }

    void testPopulateRow() {

        // Adding 1 row
        std::vector<std::string> token1 = lineParser.parseTokens("<1>");
        const char columnTypes1[2] = {ColumnType::BOOL, '\0'};
        Schema schema(columnTypes1);
        Row row(schema);
        valueFactory.populateRow(schema, token1, row);

        t_true(row.get_bool(0));

        // Add a complex row to the table with missing fields
        const char columnTypes2[6] = {ColumnType::BOOL, ColumnType::INT, ColumnType::STRING, ColumnType::FLOAT, ColumnType::INT, '\0'};
        Schema schema2(columnTypes2);
        Row row2(schema2);
        std::vector<std::string> token2 = lineParser.parseTokens("<1> <123> <\"Hello World\"> <22.22> <>");

        valueFactory.populateRow(schema2, token2, row2);

        t_true(row2.get_bool(0) == 1);
        t_true(row2.get_int(1) == 123);
        t_true(strcmp(row2.get_string(2)->c_str(), "Hello World") == 0);
        t_true(floatComparison(row2.get_float(3), 22.22));

        OK("ValueFactory.populateRow");
    }
};
#pragma once

#include "../../test_util.h"
#include "../test_template.h"
#include "../../../src/adapter/values/valueproducers.h"
#include "../../../src/dataframe/dataframe.h"

class ValueProducers_test : public Test_template {

  public:

    virtual void run() {
      testStringProducer();
      testIsValidNumber();
      testFloatProducer();
      testIntProducer();
      testBoolProducer();
    }

  private:
    StringProducer stringProducer = StringProducer();
    FloatProducer floatProducer = FloatProducer();
    IntProducer intProducer = IntProducer();
    BoolProducer boolProducer = BoolProducer();

    const char columnTypes[5] = {ColumnType::INT, ColumnType::STRING, ColumnType::BOOL, ColumnType::FLOAT, '\0'};


    void testStringProducer() {
        Schema strSchema(columnTypes);
        Row row(strSchema);

        // Test empty string
        stringProducer.produce("", row, 1);
        t_true(row.get_string(1) == nullptr);

        // Test string with quotes
        stringProducer.produce("\"test hello\"", row, 1);
        String* s1 = row.get_string(1);
        t_true(s1 != nullptr);
        t_true(strcmp(s1->c_str(), "test hello") == 0);

        // Test string with space before and after no quotes
        stringProducer.produce("test", row, 1);
        String* s2 = row.get_string(1);
        t_true(s2 != nullptr);
        t_true(strcmp(s2->c_str(), "test") == 0);

        // Test string with spaces inside and no quotes
        stringProducer.produce("te  st", row, 1);
        String* s3 = row.get_string(1);
        t_true(s3 == nullptr);

      OK("ValueProducers.StringProducer.produce");
    }

    void testIsValidNumber() {

      // Test when single digit
      bool b1 = isValidNumber("1", false);
      t_true(b1);

      // Test with single digit and allowDecimal
      bool b2 = isValidNumber("1", true);
      t_true(b2);

      // Test when decimal but not allowDecimal
      bool b3 = isValidNumber("1.0", false);
      t_false(b3);

      // Test when decimal and allowDecimal
      bool b4 = isValidNumber("1.0", true);
      t_true(b4);

      // Test with string text
      bool b5 = isValidNumber("test123", true);
      t_false(b5);

      // Test with + sign, without decimal
      bool b6 = isValidNumber("+1", false);
      t_true(b6);

      // Test with + sign, with decimal
      bool b7 = isValidNumber("+1", true);
      t_true(b7);

      // Test with + sign, without decimal
      bool b8 = isValidNumber("+1.00", false);
      t_false(b8);

      // Test with + sign, without decimal
      bool b9 = isValidNumber("+1.00", true);
      t_true(b9);

      // Test with - sign, without decimal
      bool b10 = isValidNumber("-1", false);
      t_true(b10);

      // Test with - sign, with decimal
      bool b11 = isValidNumber("-1", true);
      t_true(b11);

      // Test with - sign, without decimal
      bool b12 = isValidNumber("-1.00", false);
      t_false(b12);

      // Test with - sign, without decimal
      bool b13 = isValidNumber("-1.00", true);
      t_true(b13);

      //Test when string is empty
      bool b14 = isValidNumber("", false);
      t_true(b4);

      OK("ValueProducers.isValidNumber");
    }

    void testFloatProducer() {
        Schema strSchema(columnTypes);
        Row row(strSchema);

        // Test incorrect float
        floatProducer.produce("test12", row, 3);
        t_false(row.get_float(3));

        // Test with float
        floatProducer.produce("1.0", row, 3);
        float f1 = row.get_float(3);
        t_true(f1);
        t_true(f1 == 1.0);

        OK("ValueProducers.FloatProducer.produce");
    }

    void testIntProducer() {
        Schema strSchema(columnTypes);
        Row row(strSchema);

        // Test incorrect float
        intProducer.produce("test12", row, 0);
        t_false(row.get_int(0));

        // Test with float
        intProducer.produce("123", row, 0);
        int i1 = row.get_int(0);
        t_true(i1);
        t_true(i1 == 123);

        OK("ValueProducers.IntProducer.produce");
    }

    void testBoolProducer() {

        Schema strSchema(columnTypes);
        Row row(strSchema);

        // Test with invalid bool
        boolProducer.produce("test12", row, 2);
        t_false(row.get_bool(2));

        // Test with string = 0
        boolProducer.produce("0", row, 2);
        t_true(row.get_bool(2) == 0);

        // Test with string = 1;
        boolProducer.produce("1", row, 2);
        t_true(row.get_bool(2) == 1);

        OK("ValueProducers.BoolProducer.produce");
    }
};
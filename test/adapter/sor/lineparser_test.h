#pragma once

#include "../../test_util.h"
#include "../test_template.h"
#include "../../../src/adapter/sor/lineparser.h"

class Lineparser_test : public Test_template {

  public:

    virtual void run() {
      testParseTokens();
    }

  private:
    LineParser lineParser = LineParser();

    void testParseTokens() {
      // Test empty string
      std::vector<std::string> v1 = lineParser.parseTokens("");
      t_true(v1.size() == 0);
      v1.clear();

      // Test only one delim
      std::vector<std::string> v2 = lineParser.parseTokens("<");
      t_true(v2.size() == 0);
      v2.clear();

      // Test empty space
      std::vector<std::string> v3 = lineParser.parseTokens("   ");
      t_true(v3.size() == 0);
      v3.clear();

      // Test delim with nothing inside
      std::vector<std::string> v4 = lineParser.parseTokens("<>");
      t_true(v4.size() == 1);
      v4.clear();

      // Test delim with empty space inside
      std::vector<std::string> v5 = lineParser.parseTokens("<   >");
      t_true(v5.size() == 1);
      v5.clear();

      // Test delim with string inside
      std::vector<std::string> v6 = lineParser.parseTokens("<test123>");
      t_true(v6.size() == 1);
      t_true(v6[0] == "test123");
      v6.clear();

      // Test delim with int inside
      std::vector<std::string> v7 = lineParser.parseTokens("<123>");
      t_true(v7.size() == 1);
      t_true(v7[0] == "123");
      v7.clear();

      // Test with spaces within delim
      std::vector<std::string> v8 = lineParser.parseTokens("<  test123      >");
      t_true(v8.size() == 1);
      t_true(v8[0] == "test123");
      v8.clear();

      // Test failure with multiple delims
      std::vector<std::string> v9 = lineParser.parseTokens("<<<>");
      t_true(v9.size() == 0);
      v9.clear();

      // Test with multiple tokens
      std::vector<std::string> v10 = lineParser.parseTokens("<test123> <55> <\"Hello test\">      <yay>");
      t_true(v10.size() == 4);
      t_true(v10[0] == "test123");
      t_true(v10[1] == "55");
      t_true(v10[2] == "\"Hello test\"");
      t_true(v10[3] == "yay");
      v10.clear();
      
      OK("LineParser.parseTokens");
    }
};
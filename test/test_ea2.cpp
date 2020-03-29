#include <gtest/gtest.h>

#include "utils.h"
#include "../src/utils/datastructures/element_column.h"
#include "../src/utils/column_type.h"
#include "../src/dataframe/dataframe.h"

/* Start KVStore tests                                                */
/*-----------------------------------------------------------------*/
const char columnTypes2[5] = {ColumnType::INT, ColumnType::STRING, ColumnType::BOOL, ColumnType::DOUBLE, '\0'};

void testDataFrameEquality(DataFrame* df1, DataFrame* df2) {
    Schema s1 = df1->get_schema();
    Schema s2 = df2->get_schema();

    GT_TRUE(s1.width() == s2.width());
    GT_TRUE(df1->nrows() == df2->nrows());
    GT_TRUE(df1->ncols() == df2->ncols());
    GT_TRUE(df1->nrows() == df2->nrows());
    for (int i = 0; i < s1.width(); i++) {
        GT_TRUE(s1.col_type(i) == s2.col_type(i));
    }
    for (int i = 0; i < df1->nrows(); i++) {
        GT_TRUE(df1->get_int(0, i) == df2->get_int(0, i));
        GT_TRUE(strcmp(df1->get_string(1, i)->c_str(), df2->get_string(1, i)->c_str()) == 0);
        GT_TRUE(df1->get_bool(2, i) == df2->get_bool(2, i));
        GT_TRUE(df1->get_double(3, i) == df2->get_double(3, i));
    }
}

void testKVStoreMethods() {
    Schema schema(columnTypes2);
    DataFrame dataFrame(schema);

    double f = 0.0;
    char buffer[100];
    for (int i = 0; i < 100000; i++) {
        Row currRow(schema);

        currRow.set(0, i);
        sprintf(buffer, "ITEM%i", i);
        String currStr(buffer);
        currRow.set(1, &currStr);
        currRow.set(2, false);
        currRow.set(3, f);

        dataFrame.add_row(currRow);

        f += 1.0;
    }

    Key key("DF1", 1);

    KVStore kvStore;
    kvStore.put(&dataFrame, key);

    DataFrame* retrievedDF1 = kvStore.get(key);
    testDataFrameEquality(&dataFrame, retrievedDF1);

    DataFrame* retrievedDF2 = kvStore.waitAndGet(key);
    testDataFrameEquality(&dataFrame, retrievedDF2);

    exit(0);
}

void testMultipleKVPut() {
    Schema schema(columnTypes2);
    DataFrame dataFrame(schema);
    DataFrame dataFrame2(schema);

    double f = 0.0;
    char buffer[100];
    char buffer2[100];
    for (int i = 0; i < 100000; i++) {
        Row currRow1(schema);
        currRow1.set(0, i);
        sprintf(buffer, "ITEM%i", i);
        String currStr1(buffer);
        currRow1.set(1, &currStr1);
        currRow1.set(2, false);
        currRow1.set(3, f);
        dataFrame.add_row(currRow1);

        Row currRow2(schema);
        int df2Int = i + 10;
        double df2Double = f + 20.0;
        currRow2.set(0, df2Int);
        sprintf(buffer2, "DF2_ITEM%i", i);
        String currStr2(buffer2);
        currRow2.set(1, &currStr2);
        currRow2.set(2, true);
        currRow2.set(3, df2Double);
        dataFrame2.add_row(currRow2);

        f += 1.0;
    }

    Key key1("DF1", 1);
    Key key2("DF2", 1);

    KVStore kvStore;
    kvStore.put(&dataFrame, key1);
    kvStore.put(&dataFrame2, key2);

    DataFrame* retrievedDF1 = kvStore.get(key1);
    testDataFrameEquality(&dataFrame, retrievedDF1);

    DataFrame* retrievedDF2 = kvStore.waitAndGet(key1);
    testDataFrameEquality(&dataFrame, retrievedDF2);

    DataFrame* retrievedDF3 = kvStore.get(key2);
    testDataFrameEquality(&dataFrame2, retrievedDF3);

    DataFrame* retrievedDF4 = kvStore.get(key2);
    testDataFrameEquality(retrievedDF3, retrievedDF4);

    exit(0);
}

void testMultipleKVPutDifferentNodes() {
    Schema schema(columnTypes2);
    DataFrame dataFrame(schema);
    DataFrame dataFrame2(schema);

    double f = 0.0;
    char buffer[100];
    char buffer2[100];
    for (int i = 0; i < 100000; i++) {
        Row currRow1(schema);
        currRow1.set(0, i);
        sprintf(buffer, "ITEM%i", i);
        String currStr1(buffer);
        currRow1.set(1, &currStr1);
        currRow1.set(2, false);
        currRow1.set(3, f);
        dataFrame.add_row(currRow1);

        Row currRow2(schema);
        int df2Int = i + 10;
        double df2Double = f + 20.0;
        currRow2.set(0, df2Int);
        sprintf(buffer2, "DF2_ITEM%i", i);
        String currStr2(buffer2);
        currRow2.set(1, &currStr2);
        currRow2.set(2, true);
        currRow2.set(3, df2Double);
        dataFrame2.add_row(currRow2);

        f += 1.0;
    }

    Key key1("DF1", 1);
    Key key2("DF2", 2);

    KVStore kvStore;
    kvStore.put(&dataFrame, key1);
    kvStore.put(&dataFrame2, key2);

    DataFrame* retrievedDF1 = kvStore.get(key1);
    testDataFrameEquality(&dataFrame, retrievedDF1);

    DataFrame* retrievedDF2 = kvStore.waitAndGet(key1);
    testDataFrameEquality(&dataFrame, retrievedDF2);

    DataFrame* retrievedDF3 = kvStore.get(key2);
    testDataFrameEquality(&dataFrame2, retrievedDF3);

    DataFrame* retrievedDF4 = kvStore.get(key2);
    testDataFrameEquality(retrievedDF3, retrievedDF4);

    exit(0);
}

void testStoreDoesntDeadlock() {
    Schema schema(columnTypes2);
    DataFrame dataFrame(schema);

    Key k("hi", 0);

    std::vector<std::thread> threads;
    KVStore kvStore;

    for (size_t i = 0; i < 100; i++) {
        threads.emplace_back(std::thread([&k, &kvStore] {
            kvStore.waitAndGet(k);
        }));
    }

    kvStore.put(&dataFrame, k);

    for (size_t i = 0; i < threads.size(); i++) { threads[i].join(); }
    exit(0);
}

TEST(W3, testKVStoreMethods) { ASSERT_EXIT_ZERO(testKVStoreMethods) }
TEST(W3, testMultipleKVPut) { ASSERT_EXIT_ZERO(testMultipleKVPut) }
TEST(W3, testMultipleKVPutDifferentNodes) { ASSERT_EXIT_ZERO(testMultipleKVPutDifferentNodes) }
TEST(W3, testStoreDoesntDeadlock) { ASSERT_EXIT_ZERO(testStoreDoesntDeadlock) }
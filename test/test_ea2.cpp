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

    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    for (size_t i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;
    }

    KVStore& kvStore = *stores[0];
    kvStore.put(&dataFrame, key);

    DataFrame* retrievedDF1 = kvStore.get(key);
    testDataFrameEquality(&dataFrame, retrievedDF1);

    DataFrame* retrievedDF2 = kvStore.waitAndGet(key);
    testDataFrameEquality(&dataFrame, retrievedDF2);

    for (size_t i = 0; i < stores.size(); i++) {
        delete stores[i];
    }

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

    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    for (size_t i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;
    }

    KVStore& kvStore = *stores[0];

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

    for (size_t i = 0; i < stores.size(); i++) {
        delete stores[i];
    }

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

    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    for (size_t i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;
    }

    KVStore& kvStore = *stores[0];

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

    for (size_t i = 0; i < stores.size(); i++) {
        delete stores[i];
    }

    exit(0);
}

void testStoreDoesntDeadlock() {
    Schema schema(columnTypes2);
    DataFrame dataFrame(schema);

    Key k("hi", 0);

    std::vector<std::thread> threads;
    KVStore kvStore;
    kvStore._stores = { &kvStore };

    for (size_t i = 0; i < 100; i++) {
        threads.emplace_back(std::thread([&k, &kvStore] {
            kvStore.waitAndGet(k);
        }));
    }

    kvStore.put(&dataFrame, k);

    for (size_t i = 0; i < threads.size(); i++) { threads[i].join(); }
    exit(0);
}

void testFromArray() {
    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    for (size_t i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;
    }

    KVStore& kvStore = *stores[0];

    Key d("DOUBLE", 0);
    Key i("INT", 0);
    Key b("BOOL", 0);
    Key s("STRING", 0);

    double ds[] = { 12.0, -231.22, 293939.12378 };
    int is[] = { 12, -231, 293939 };
    bool bs[] = { false, true, false };
    String* ss[] = { new String("HI"), new String("BYE"), new String("ASDHJKAHSJKDHJK") };

    DataFrame::fromArray(&d, &kvStore, 3, ds);
    DataFrame::fromArray(&i, &kvStore, 3, is);
    DataFrame::fromArray(&b, &kvStore, 3, bs);
    DataFrame::fromArray(&s, &kvStore, 3, ss);

    DataFrame* dd = kvStore.get(d);
    DataFrame* id = kvStore.get(i);
    DataFrame* bd = kvStore.get(b);
    DataFrame* sd = kvStore.get(s);

    for (size_t i = 0; i < 3; i++) {
        GT_TRUE(dd->get_double(0, i) == ds[i]);
        GT_TRUE(id->get_int(0, i) == is[i]);
        GT_TRUE(bd->get_bool(0, i) == bs[i]);
        GT_TRUE(sd->get_string(0, i)->equals(ss[i]));
    }

    GT_TRUE(dd->ncols() == 1);
    GT_TRUE(dd->nrows() == 3);

    GT_TRUE(id->ncols() == 1);
    GT_TRUE(id->nrows() == 3);

    GT_TRUE(bd->ncols() == 1);
    GT_TRUE(bd->nrows() == 3);

    GT_TRUE(sd->ncols() == 1);
    GT_TRUE(sd->nrows() == 3);

    delete dd;
    delete id;
    delete bd;
    delete sd;

    for (size_t i = 0; i < 3; i++) {
        delete ss[i];
    }

    for (size_t i = 0; i < stores.size(); i++) {
        delete stores[i];
    }

    exit(0);
}

void testFromScalar() {
    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    for (size_t i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;
    }

    KVStore& kvStore = *stores[0];

    Key d("DOUBLE", 0);
    Key i("INT", 0);
    Key b("BOOL", 0);
    Key s("STRING", 0);

    double dv = 4848439.324234;
    int iv = -458930495;
    bool bv = true;
    String sv("Hello");

    DataFrame::fromScalar(&d, &kvStore, dv);
    DataFrame::fromScalar(&i, &kvStore, iv);
    DataFrame::fromScalar(&b, &kvStore, bv);
    DataFrame::fromScalar(&s, &kvStore, &sv);

    DataFrame* dd = kvStore.get(d);
    DataFrame* id = kvStore.get(i);
    DataFrame* bd = kvStore.get(b);
    DataFrame* sd = kvStore.get(s);

    GT_TRUE(dd->get_double(0, 0) == dv);
    GT_TRUE(id->get_int(0, 0) == iv);
    GT_TRUE(bd->get_bool(0, 0) == bv);
    GT_TRUE(sd->get_string(0, 0)->equals(&sv));

    GT_TRUE(dd->ncols() == 1);
    GT_TRUE(dd->nrows() == 1);

    GT_TRUE(id->ncols() == 1);
    GT_TRUE(id->nrows() == 1);

    GT_TRUE(bd->ncols() == 1);
    GT_TRUE(bd->nrows() == 1);

    GT_TRUE(sd->ncols() == 1);
    GT_TRUE(sd->nrows() == 1);

    delete dd;
    delete id;
    delete bd;
    delete sd;

    for (size_t i = 0; i < stores.size(); i++) {
        delete stores[i];
    }

    exit(0);
}

TEST(W3, testKVStoreMethods) { ASSERT_EXIT_ZERO(testKVStoreMethods) }
TEST(W3, testMultipleKVPut) { ASSERT_EXIT_ZERO(testMultipleKVPut) }
TEST(W3, testMultipleKVPutDifferentNodes) { ASSERT_EXIT_ZERO(testMultipleKVPutDifferentNodes) }
TEST(W3, testStoreDoesntDeadlock) { ASSERT_EXIT_ZERO(testStoreDoesntDeadlock) }
TEST(W3, testFromArray) { ASSERT_EXIT_ZERO(testFromArray) }
TEST(W3, testFromScalar) { ASSERT_EXIT_ZERO(testFromScalar) }
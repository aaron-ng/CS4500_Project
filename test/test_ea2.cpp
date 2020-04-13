#include <gtest/gtest.h>

#include "utils.h"
#include "../src/dataframe/dataframe.h"
#include "../src/network/server.h"

/* Start KVStore tests                                                */
/*-----------------------------------------------------------------*/
const char columnTypes2[5] = {ColumnType::INT, ColumnType::STRING, ColumnType::BOOL, ColumnType::DOUBLE, '\0'};

void testDataFrameEquality(DataFrame* df1, DataFrame* df2) {
    Schema s1 = df1->get_schema();
    Schema s2 = df2->get_schema();

    assert(s1.width() == s2.width());
    assert(df1->nrows() == df2->nrows());
    assert(df1->ncols() == df2->ncols());
    assert(df1->nrows() == df2->nrows());
    for (int i = 0; i < s1.width(); i++) {
        assert(s1.col_type(i) == s2.col_type(i));
    }
    for (int i = 0; i < df1->nrows(); i++) {
        assert(df1->get_int(0, i) == df2->get_int(0, i));
        assert(strcmp(df1->get_string(1, i)->c_str(), df2->get_string(1, i)->c_str()) == 0);
        assert(df1->get_bool(2, i) == df2->get_bool(2, i));
        assert(df1->get_double(3, i) == df2->get_double(3, i));
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
        currRow.set(1, new String(buffer));
        currRow.set(2, false);
        currRow.set(3, f);

        dataFrame.add_row(currRow);

        f += 1.0;
    }

    Key key("DF1", 1);

    storeOperation([&](std::vector<KVStore*>& stores) {
        KVStore& kvStore = *stores[0];
        kvStore.put(&dataFrame, key);

        DataFrame* retrievedDF1 = kvStore.get(key);
        testDataFrameEquality(&dataFrame, retrievedDF1);

        DataFrame* retrievedDF2 = kvStore.waitAndGet(key);
        testDataFrameEquality(&dataFrame, retrievedDF2);

        delete retrievedDF1;
        delete retrievedDF2;

        return true;
    });

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
        currRow1.set(1, new String(buffer));
        currRow1.set(2, false);
        currRow1.set(3, f);
        dataFrame.add_row(currRow1);

        Row currRow2(schema);
        int df2Int = i + 10;
        double df2Double = f + 20.0;
        currRow2.set(0, df2Int);
        sprintf(buffer2, "DF2_ITEM%i", i);
        currRow2.set(1, new String(buffer2));
        currRow2.set(2, true);
        currRow2.set(3, df2Double);
        dataFrame2.add_row(currRow2);

        f += 1.0;
    }

    Key key1("DF1", 1);
    Key key2("DF2", 1);


    storeOperation([&](std::vector<KVStore*>& stores) {
        KVStore &kvStore = *stores[0];

        kvStore.put(&dataFrame, key1);
        kvStore.put(&dataFrame2, key2);

        DataFrame *retrievedDF1 = kvStore.get(key1);
        testDataFrameEquality(&dataFrame, retrievedDF1);

        DataFrame *retrievedDF2 = kvStore.waitAndGet(key1);
        testDataFrameEquality(&dataFrame, retrievedDF2);

        DataFrame *retrievedDF3 = kvStore.get(key2);
        testDataFrameEquality(&dataFrame2, retrievedDF3);

        DataFrame *retrievedDF4 = kvStore.get(key2);
        testDataFrameEquality(retrievedDF3, retrievedDF4);

        delete retrievedDF1;
        delete retrievedDF2;
        delete retrievedDF3;
        delete retrievedDF4;

        return true;
    });

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
        currRow1.set(1, new String(buffer));
        currRow1.set(2, false);
        currRow1.set(3, f);
        dataFrame.add_row(currRow1);

        Row currRow2(schema);
        int df2Int = i + 10;
        double df2Double = f + 20.0;
        currRow2.set(0, df2Int);
        sprintf(buffer2, "DF2_ITEM%i", i);
        currRow2.set(1, new String(buffer2));
        currRow2.set(2, true);
        currRow2.set(3, df2Double);
        dataFrame2.add_row(currRow2);

        f += 1.0;
    }

    Key key1("DF1", 1);
    Key key2("DF2", 2);

    storeOperation([&](std::vector<KVStore*>& stores) {

        KVStore &kvStore = *stores[0];

        kvStore.put(&dataFrame, key1);
        kvStore.put(&dataFrame2, key2);

        DataFrame *retrievedDF1 = kvStore.get(key1);
        testDataFrameEquality(&dataFrame, retrievedDF1);

        DataFrame *retrievedDF2 = kvStore.waitAndGet(key1);
        testDataFrameEquality(&dataFrame, retrievedDF2);

        DataFrame *retrievedDF3 = kvStore.get(key2);
        testDataFrameEquality(&dataFrame2, retrievedDF3);

        DataFrame *retrievedDF4 = kvStore.get(key2);
        testDataFrameEquality(retrievedDF3, retrievedDF4);

        delete retrievedDF1;
        delete retrievedDF2;
        delete retrievedDF3;
        delete retrievedDF4;

        return true;
    });

    exit(0);
}

void testStoreDoesntDeadlock() {
    Schema schema(columnTypes2);
    DataFrame dataFrame(schema);

    Key k("hi", 0);

    std::vector<std::thread> threads;
    storeOperation([&](std::vector<KVStore*>& stores) {

        KVStore& kvStore = *stores[2];
        for (size_t i = 0; i < 10; i++) {
            threads.emplace_back(std::thread([&k, &kvStore] {
                kvStore.waitAndGet(k);
            }));
        }

        kvStore.put(&dataFrame, k);

        for (size_t i = 0; i < threads.size(); i++) { threads[i].join(); }
        return true;
    });

    exit(0);
}

void testFromArray() {
    storeOperation([&](std::vector<KVStore*>& stores) -> bool {

        KVStore &kvStore = *stores[0];

        Key d("DOUBLE", 0);
        Key i("INT", 0);
        Key b("BOOL", 0);
        Key s("STRING", 0);

        double ds[] = {12.0, -231.22, 293939.12378};
        int is[] = {12, -231, 293939};
        bool bs[] = {false, true, false};
        String *ss[] = {new String("HI"), new String("BYE"), new String("ASDHJKAHSJKDHJK")};

        DataFrame::fromArray(&d, &kvStore, 3, ds);
        DataFrame::fromArray(&i, &kvStore, 3, is);
        DataFrame::fromArray(&b, &kvStore, 3, bs);
        DataFrame::fromArray(&s, &kvStore, 3, ss);

        DataFrame *dd = kvStore.get(d);
        DataFrame *id = kvStore.get(i);
        DataFrame *bd = kvStore.get(b);
        DataFrame *sd = kvStore.get(s);

        for (size_t i = 0; i < 3; i++) {
            assert(dd->get_double(0, i) == ds[i]);
            assert(id->get_int(0, i) == is[i]);
            assert(bd->get_bool(0, i) == bs[i]);
            assert(sd->get_string(0, i)->equals(ss[i]));
        }

        assert(dd->ncols() == 1);
        assert(dd->nrows() == 3);

        assert(id->ncols() == 1);
        assert(id->nrows() == 3);

        assert(bd->ncols() == 1);
        assert(bd->nrows() == 3);

        assert(sd->ncols() == 1);
        assert(sd->nrows() == 3);

        delete dd;
        delete id;
        delete bd;
        delete sd;

        for (size_t i = 0; i < 3; i++) {
            delete ss[i];
        }

        return true;
    });

    exit(0);
}

void testFromScalar() {
    storeOperation([&](std::vector<KVStore*>& stores) -> bool {
        KVStore &kvStore = *stores[0];

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

        DataFrame *dd = kvStore.get(d);
        DataFrame *id = kvStore.get(i);
        DataFrame *bd = kvStore.get(b);
        DataFrame *sd = kvStore.get(s);

        assert(dd->get_double(0, 0) == dv);
        assert(id->get_int(0, 0) == iv);
        assert(bd->get_bool(0, 0) == bv);
        assert(sd->get_string(0, 0)->equals(&sv));

        assert(dd->ncols() == 1);
        assert(dd->nrows() == 1);

        assert(id->ncols() == 1);
        assert(id->nrows() == 1);

        assert(bd->ncols() == 1);
        assert(bd->nrows() == 1);

        assert(sd->ncols() == 1);
        assert(sd->nrows() == 1);

        delete dd;
        delete id;
        delete bd;
        delete sd;

        return true;
    });

    exit(0);
}

void testFromFile() {
    storeOperation([&](std::vector<KVStore*>& stores) -> bool {
        KVStore &kvStore = *stores[0];

        Key file("FILE", 0);
        DataFrame::fromFile("data/commits.ltgt", &file, &kvStore);

        DataFrame* data = kvStore.get(file);
        assert(data->ncols() == 3);
        assert(data->nrows() == 8);
        assert(!strcmp(data->get_schema()._types, "III"));

        int projects[] = {0, 0, 2, 2, 3, 3, 1, 1};
        int authors[] = {0, 1, 0, 4967, 2, 0, 2, 3};

        for (int i = 0; i < data->nrows(); i++) {
            assert(data->get_int(0, i) == projects[i]);
            assert(data->get_int(1, i) == authors[i]);
            assert(data->get_int(2, i) == authors[i]);
        }

        delete data;
        return true;
    });

    exit(0);
}

TEST(W3, testKVStoreMethods) { ASSERT_EXIT_ZERO(testKVStoreMethods) }
TEST(W3, testMultipleKVPut) { ASSERT_EXIT_ZERO(testMultipleKVPut) }
TEST(W3, testMultipleKVPutDifferentNodes) { ASSERT_EXIT_ZERO(testMultipleKVPutDifferentNodes) }
TEST(W3, testStoreDoesntDeadlock) { ASSERT_EXIT_ZERO(testStoreDoesntDeadlock) }
TEST(W3, testFromArray) { ASSERT_EXIT_ZERO(testFromArray) }
TEST(W3, testFromScalar) { ASSERT_EXIT_ZERO(testFromScalar) }
TEST(W3, testFromFile) { ASSERT_EXIT_ZERO(testFromFile) }
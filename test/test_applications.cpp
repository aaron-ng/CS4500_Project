#include <thread>
#include <gtest/gtest.h>

#include "utils.h"
#include "../src/demo.h"
#include "../src/network/server.h"
#include "../src/linus.h"

void testDemo() {
    storeOperation([](std::vector<KVStore*>& stores) {
        std::vector<std::thread> threads;

        for (int i = 0; i < stores.size(); i++) {
            threads.emplace_back(std::thread([i, &stores]() {
                Demo demo(i, *stores[i]);
                demo._run();
            }));
        }

        for (int i = 0; i < 3; i++) {
            threads[i].join();
        }

        KVStore* kv = stores[0];
        DataFrame* result = kv->get(verify);
        DataFrame* expected = kv->get(check);

        assert(expected->get_double(0,0)==result->get_double(0,0));
        delete result;
        delete expected;

        return true;
    });

    exit(0);
}

void testLinus() {

    const char* PROJ = "../data/projects.ltgt";
    const char* USER = "../data/users.ltgt";
    const char* COMM = "../data/commits.ltgt";
    size_t NUM_NODES = 3;

    storeOperation([&](std::vector<KVStore*>& stores) {
        std::vector<std::thread> threads;

        for (int i = 0; i < stores.size(); i++) {
            threads.emplace_back(std::thread([i, &stores, PROJ, USER, COMM, NUM_NODES]() {
                Linus demo(i, *stores[i], PROJ, USER, COMM, NUM_NODES);
                demo._run();
            }));
        }

        for (int i = 0; i < 3; i++) {
            threads[i].join();
        }

        KVStore* kv = stores[0];

        Key level0Key("users-0-0");
        DataFrame* level0 = kv->get(level0Key);

        assert(level0->nrows() == 1);
        assert(level0->get_int(0, 0) == 4967);

        Key level1Key("users-1-0");
        DataFrame* level1 = kv->get(level1Key);

        assert(level1->nrows() == 1);
        assert(level1->get_int(0, 0) == 0);

        Key level2Key("users-2-0");
        DataFrame* level2 = kv->get(level2Key);

        assert(level2->nrows() == 2);
        assert(level2->get_int(0, 0) == 1);
        assert(level2->get_int(0, 1) == 2);

        Key level3Key("users-3-0");
        DataFrame* level3 = kv->get(level3Key);

        assert(level3->nrows() == 1);
        assert(level3->get_int(0, 0) == 3);

        delete level0;
        delete level1;
        delete level2;
        delete level3;

        return true;
    });

    exit(0);
}

TEST(W7, testDemo) { ASSERT_EXIT_ZERO(testDemo) }
TEST(W7, testLinus) { ASSERT_EXIT_ZERO(testLinus) }
#include <thread>
#include <gtest/gtest.h>

#include "utils.h"
#include "../src/demo.h"
#include "../src/network/server.h"

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

        GT_TRUE(expected->get_double(0,0)==result->get_double(0,0));
        delete result;
        delete expected;
    });

    exit(0);
}

TEST(W7, testDemo) { ASSERT_EXIT_ZERO(testDemo) }
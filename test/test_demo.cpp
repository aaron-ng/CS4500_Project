//#include <thread>
//#include <gtest/gtest.h>
//
//#include "utils.h"
//#include "../src/demo.h"
//
//void testDemo() {
//    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
//    std::vector<std::thread> threads;
//
//    for (int i = 0; i < stores.size(); i++) {
//        stores[i]->_stores = stores;
//
//        threads.push_back(std::thread([i, &stores]() {
//            Demo demo(i, *stores[i]);
//            demo._run();
//        }));
//    }
//
//    for (int i = 0; i < 3; i++) {
//        threads[i].join();
//    }
//
//    KVStore* kv = stores[0];
//    DataFrame* result = kv->get(verify);
//    DataFrame* expected = kv->get(check);
//
//    GT_TRUE(expected->get_double(0,0)==result->get_double(0,0));
//    delete result;
//    delete expected;
//
//    for (int i = 0; i < 3; i++) {
//        delete stores[i];
//    }
//
//    exit(0);
//}
//
//TEST(W7, testDemo) { ASSERT_EXIT_ZERO(testDemo) }
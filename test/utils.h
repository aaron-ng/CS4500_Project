#pragma once

#include "../src/ea2/kvstore/kvstore.h"
#include "../src/network/server.h"

#define GT_TRUE(a)   ASSERT_EQ((a),true)
#define GT_FALSE(a)  ASSERT_EQ((a),false)
#define ASSERT_EXIT_ZERO(a) ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

inline void storeOperation(std::function<void(std::vector<KVStore*>&)> op) {
    Server server(inet_addr("127.0.0.1"), SERVER_PORT);
    std::thread serverThread([&] {
        server.run();
    });

    sleep(1);

    std::vector<KVStore*> stores = {
            new KVStore(inet_addr("127.0.0.1"), 25565, inet_addr("127.0.0.1"), SERVER_PORT),
            new KVStore(inet_addr("127.0.0.1"), 25566, inet_addr("127.0.0.1"), SERVER_PORT),
            new KVStore(inet_addr("127.0.0.1"), 25567, inet_addr("127.0.0.1"), SERVER_PORT)
    };

    sleep(1);

    op(stores);

    server.close();
    serverThread.join();

    for (size_t i = 0; i < stores.size(); i++) {
        delete stores[i];
    }
}
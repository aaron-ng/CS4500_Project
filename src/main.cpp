// Language C++

#include <thread>
#include "demo.h"
#include "word_count.h"
#include "network/server.h"

int main(int argc, char** argv) {
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

    std::vector<std::thread> threads;

    for (int i = 0; i < stores.size(); i++) {
        threads.emplace_back(std::thread([i, &stores]() {
            WordCount wordCount(i, *stores[i]);
            wordCount._run();
        }));
    }

    for (int i = 0; i < 3; i++) {
        threads[i].join();
    }

    for (int i = 0; i < 3; i++) {
        delete stores[i];
    }

    server.close();
    serverThread.join();



    return 0;
}
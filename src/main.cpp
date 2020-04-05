// Language C++

#include <thread>
#include "demo.h"
#include "network/server.h"

void runServer() {
    Server server(inet_addr("127.0.0.1"), SERVER_PORT);
    server.run();
}

int main(int argc, char** argv) {
    std::thread serverThread(&runServer);
    sleep(1);

    std::vector<KVStore*> stores = {
            new KVStore(inet_addr("127.0.0.1"), 25565, inet_addr("127.0.0.1"), SERVER_PORT),
            new KVStore(inet_addr("127.0.0.1"), 25566, inet_addr("127.0.0.1"), SERVER_PORT),
            new KVStore(inet_addr("127.0.0.1"), 25567, inet_addr("127.0.0.1"), SERVER_PORT)
    };

    sleep(1);

    std::vector<std::thread> threads;

    for (int i = 0; i < stores.size(); i++) {
        threads.push_back(std::thread([i, &stores]() {
            Demo demo(i, *stores[i]);
            demo._run();
        }));
    }

    for (int i = 0; i < 3; i++) {
        threads[i].join();
    }

    for (int i = 0; i < 3; i++) {
        delete stores[i];
    }

    return 0;
}




// Language C++

#include <thread>
#include "demo.h"

int main(int argc, char** argv) {

    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    std::vector<std::thread> threads;

    for (int i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;

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




// Language C++

#include "linus.h"
#include "network/shared/network.h"
#include "ea2/kvstore/kvstore.h"

int main(int argc, char** argv) {
    assert(argc > 1);
    uint32_t port = atoi(argv[1]);
    KVStore store(inet_addr("127.0.0.1"), port, inet_addr("127.0.0.1"), SERVER_PORT);

    Linus(0, store)._run();

    return 0;
}
// Language C++

#include "linus.h"
#include "network/shared/network.h"

int main(int argc, char** argv) {
    assert(argc > 2);
    in_addr_t ip = inet_addr(argv[1]);
    uint32_t port = atoi(argv[2]);

    KVStore store(inet_addr("127.0.0.1"), port, ip, SERVER_PORT);

    const char* PROJ = "./data/projects.ltgt";
    const char* USER = "./data/users.ltgt";
    const char* COMM = "./data/commits.ltgt";
    size_t NUM_NODES = 3;

    while (store._byteStore._client.connectedClients() != NUM_NODES) { }

    Linus(0, store, PROJ, USER, COMM, NUM_NODES).run();

    return 0;
}
#pragma once

#include <iostream>
#include "../utils/instructor-provided/helper.h"
#include "kvstore/kvstore.h"

/**
 * An application that is running on 
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu 
 */
 class Application: public Sys {
    public:

        /** The distributed key store that this application is connected to */
        KVStore& kv;

        /**
         * Creates a new application that runs on a distributed key store. 
         * This constructor will rendezvous with the central server.
         * @param idx An unused parameter so far. The client ID will be assigned by the server,
         * but the example code supplies this param
         * @param kv The KV store to use for this application
         */
        Application(size_t idx, KVStore& kv): kv(kv) {  }

        /**
         * Provides the node identifier of the running application. This is determined
         * by the rendezvous server
         */
        size_t this_node() const {  return kv.this_node(); }

        /** The function that is called after the application setup is complete */
        virtual void _run() = 0;

        /** Runs the application */
        void run() {
            _run();
            if (this_node() == 0) {
                kv._byteStore._client.teardownSystem();
            }
            while (kv._byteStore._client.connected()) { usleep(50); }
        }

};
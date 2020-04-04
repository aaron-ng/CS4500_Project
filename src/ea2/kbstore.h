#pragma once

// Language: C++

#include <thread>

#include "../network/client.h"
#include "../utils/key.h"

/**
 * A byte array with a length and contents
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class ByteArray: public Object {
    public:

        /** The contents of the buffer */
        const char* contents;

        /** The length of the buffer */
        size_t length;

        /** Default constructor */
        ByteArray(const char *contents, size_t length) : contents(contents), length(length) {}

        virtual ~ByteArray() {
            delete[] contents;
        }

};

/**
 * An object wrapper a std::atomic that says if a key is ready
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Ready: public Object {
    public:
        std::atomic<bool> isReady;

        /** Default constructor */
        Ready() : isReady(false) {}
};

/**
 * A key to byte array store. All arrays are owned by the store
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class KBStore {
    public:
        /** The map where all of the bytes are stored */
        Map _map;

        /** Statuses of keys that are used for waitAndGet. This prevents deadlocks */
        Map _statuses;

        /** Mutex for _statuses */
        std::mutex _statusMutex;

        /** The index of this node on the server */
        size_t _nodeIndex = 0;

        /** The client used to talk to other KBstores */
        Client _client;

        KBStore(in_addr_t ip, uint16_t port) : _client(ip, port, new KBStoreMessageHander(*this)) {}

        ~KBStore() {
            std::vector<Entry*>& entries = _map.entrySet();
            for (size_t i = 0; i < _map.get_size(); i++) {
                delete entries[i]->key;
                delete entries[i]->value;
            }
        }

        /**
         * Retrieves the buffer with the given key from the store. If the
         * value does not exist, nullptr is returned. The buffer is owned by this store.
         * @param key The key of the buffer to return
         */
        ByteArray* get(Key& key) {
            if (key._node == _nodeIndex) {
                return (ByteArray*)_map.get(&key);
            } else {
                // NETWORKING
            }
        }

        /**
         * Retrieves the buffer with the given key from the store. This call will block until the value exists
         * @param key The key of the buffer to return
         */
        ByteArray* waitAndGet(Key& key) {
            if (key._node == _nodeIndex) {
                _statusMutex.lock();

                if (_map.contains_key(&key)) {
                    _statusMutex.unlock();
                    return get(key);
                }

                Ready* ready = dynamic_cast<Ready*>(_statuses.get(&key));
                if (!ready) {
                    ready = new Ready();
                    _statuses.put(key.clone(), ready);
                }

                _statusMutex.unlock();
                while (ready->isReady == false) {}
                return get(key);
            } else {
                // NETWORKING
                return nullptr;
            }
        }

        /**
         * Puts a series of bytes inside of the store
         * @param contents The buffer to put into the store
         * @param length The length of the bytes in the buffer
         * @param key The key to store the buffer under
         */
        void put(const char *contents, size_t length, Key& key) {
            if (key._node == _nodeIndex) {

                _statusMutex.lock();

                _map.put(key.clone(), new ByteArray(contents, length));

                Ready* ready = dynamic_cast<Ready*>(_statuses.get(&key));
                if (ready) { ready->isReady = true; }

                _statusMutex.unlock();
            }
        }

        /**
         * Provides the node identifier of the running application. This is determined
         * by the rendezvous server
         */
        size_t this_node() const { return _nodeIndex; }

        /**
         * A message handler that performs all of the operations on the KVStore
         * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
         */
        class KBStoreMessageHander: public MessageHandler {
            public:

                /** The store to perform operations on */
                KBStore& _store;

                /** Default constructor */
                KBStoreMessageHander(KBStore &store) : _store(store) {}

                /**
                 * Handles the KBMessage. This does the various operations like put get and wait and get
                 * @param message
                 * @param connectedClient
                 */
                virtual void handleMessage(Message *message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message->contentSize, message->contents);
                    KBMessage kbMessage;
                    kbMessage.deserialize(deserializer);

                    switch (kbMessage.getKbMessageType()) {
                        case PUT:
                            handlePut(kbMessage, connectedClient);
                            break;
                        case GET:
                            handleGet(kbMessage, connectedClient);
                            break;
                        case GET_AND_WAIT:
                            handleWaitAndGet(kbMessage, connectedClient);
                            break;
                        default:
                            break;
                    }

                }

                /**
                 * Handles putting data inside of the store
                 * @param message The data as well as the key
                 * @param connectedClient The connected client
                 */
                void handlePut(KBMessage& message, RemoteClient &connectedClient) {

                }

                /**
                 * Handles getting data out of the store. If the data is not in the store, 0 bytes are returned
                 * @param message The data as well as the key
                 * @param connectedClient The connected client
                 */
                void handleGet(KBMessage& message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message.length(), message.getData());
                    Key* key = deserializer.read_key();

                    sendResponse(_store.get(*key), connectedClient);

                    delete key;
                }

                /**
                 * Handles getting data out of the store
                 * @param message The data as well as the key
                 * @param connectedClient The connected client
                 */
                void handleWaitAndGet(KBMessage& message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message.length(), message.getData());
                    Key* key = deserializer.read_key();

                    sendResponse(_store.waitAndGet(*key), connectedClient);

                    delete key;
                }

                /**
                 * Sends the byte array to the given client. If the byte array is empty, a response data with 0 length
                 * is sent
                 * @param bytes The bytes to send
                 * @param connectedClient The client to send the bytes to
                 */
                void sendResponse(ByteArray* bytes, RemoteClient& connectedClient) {
                    if (!bytes) {
                        KBMessage reply(RESPONSE_DATA, nullptr, 0);
                        connectedClient.send(reply);
                    } else {
                        KBMessage reply(RESPONSE_DATA, bytes->contents, bytes->length, false);
                        connectedClient.send(reply);
                    }
                }

        };

};
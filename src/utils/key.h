#pragma once

/**
 * A key that contains a name and a home node
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Key: public Object {
    public:
        String* _name;
        size_t _node;

        /**
         * Creates a new key
         * @param name The name of the key
         * @param node the home node of the key
         */
        Key(const char* name, size_t node) {
            _name = new String(name);
            _node = node;
        }

        ~Key() {
            delete _name;
        }

        /** Provides the name of the key */
        const char* getName() const {
            return _name->c_str();
        }

        /** Provides the name of the key as a string */
        String* getNameAsString() const {
            return _name;
        }

        /** Provides the home node of the key */
        size_t getNode() const {
            return _node;
        }

        /** Compute the hash code (subclass responsibility) */
        virtual size_t hash_me() { return _name->hash() + _node; };

        /** Subclasses should redefine */
        virtual bool equals(Object* other) {
            Key* givenKey = dynamic_cast<Key*>(other);
            if (givenKey == nullptr) {
                return false;
            }

            return _name->equals(givenKey->_name) && _node == givenKey->getNode();
        }

};
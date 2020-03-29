#include <iostream>
#include "../src/utils/datastructures/map.h"
#include "../src/utils/instructor-provided/string.h"


void FAIL() {   exit(1);    }
void t_true(bool p) { if (!p) FAIL(); }
void t_false(bool p) { if (p) FAIL(); }


/**
 * test cases for put()
 * program won't error when objects are put into a map
 */
void test1() {
    Map* h1 = new Map();
    String * key_1 = new String("Hello");
    String * val_1 = new String("1");
    String * key_2 = new String("World");
    String * val_2 = new String("2");
    h1->put(key_1, val_1);
    h1->put(key_2, val_2);
    delete key_1;
    delete key_2;
    delete val_1;
    delete val_2;
}

/**
 * test cases for put() and get_size() method when using the same key
 *  the map's size remains the same, the previous value should be overwritten
 */
void test2() {
    Map* h1 = new Map();
    String * key_1 = new String("Hello");
    String * val_1 = new String("1");
    String * val_2 = new String("2");
    h1->put(key_1, val_1);
    t_true(h1->get_size() == 1);
    h1->put(key_1, val_2);
    t_true(h1->get_size() == 1);
    t_false(val_1 -> equals(h1->get(key_1)));

    delete key_1;
    delete val_1;
    delete val_2;
}

/**
 * test cases for get() methods
 * comparing value extracting from Map with input value
 */
void test3() {
    Map* h1 = new Map();
    String * key_1 = new String("Hello");
    String * val_1 = new String("1");
    String * key_2 = new String("World");
    String * val_2 = new String("2");
    h1->put(key_1, val_1);
    h1->put(key_2, val_2);
    t_true(h1->get(key_1)->equals(val_1));
    t_true(h1->get(key_2)->equals(val_2));
    delete key_1;
    delete key_2;
    delete val_1;
    delete val_2;
}

/**
 * get() returns nullptr if there's no mapping for the given key in the map
 */
void test4() {
    Map* h1 = new Map();
    t_true(h1 -> get(new String("1")) == nullptr);
    delete h1;
}

/**
 * test cases for contains_key() method
 * contains_key returns true/false if the key exist/not exist in the map
 */
void test5() {
    Map* h1 = new Map();
    String * key_1 = new String("Hello");
    String * val_1 = new String("1");
    String * key_2 = new String("World");
    String * val_2 = new String("2");
    String * key_3 = new String("NEU");
    h1->put(key_1, val_1);
    h1->put(key_2, val_2);
    t_true(h1->contains_key(key_1));
    t_true(h1->contains_key(key_2));
    t_false(h1->contains_key(key_3));
    delete key_1;
    delete key_2;
    delete val_1;
    delete val_2;
    delete key_3;
    delete h1;
}


/**
 * test cases values() function
 * testing on values() function that return all values that exist in the Map
 */
void test6() {
    Map* h1 = new Map();
    String * key_1 = new String("A");
    String * val_1 = new String("1");
    String * key_2 = new String("B");
    String * val_2 = new String("2");
    String * key_3 = new String("C");
    String * val_3 = new String("3");
    h1->put(key_1, val_1);
    h1->put(key_2, val_2);
    h1->put(key_3, val_3);

    std::vector<Entry*>& entries = h1->entrySet();

    for (int i=0; i< 3; i++) {
        t_true(val_1 -> equals(entries[i]->value) || val_2 -> equals(entries[i]->value) || val_3 -> equals(entries[i]->value));
    }
    delete key_1;
    delete key_2;
    delete key_3;
    delete val_1;
    delete val_2;
    delete val_3;
    delete h1;
}

static const int _stressTestVal = 10000;

void mapStressTest() {
    Map* h1 = new Map();

    char buffer[100];
    for (int i = 0; i < _stressTestVal; i++) {
        sprintf(buffer, "test%i", i);
        String *keyStr = new String(buffer);

        sprintf(buffer, "test%i", i);
        String *valStr = new String(buffer);

        // Test put
        h1->put(keyStr, valStr);
    }

    t_true(h1->get_size() == _stressTestVal);

    for (int i = 0; i < _stressTestVal; i++) {
        sprintf(buffer, "test%i", i);
        String *keyStr = new String(buffer);

        sprintf(buffer, "test%i", i);
        String *valStr = new String(buffer);

        // Test contain
        t_true(h1->contains_key(keyStr));

        // Test get
        String* getString = dynamic_cast<String*>(h1->get(keyStr));
        t_true(getString != nullptr);
        t_true(getString->equals(valStr));
    }
}

int main() {
    test1();
    test2();
    test3();
    test5();
    test6();
	mapStressTest();
    return 0;
}

#pragma once
#include "../src/map.h"
#include "../src/string.h"
#include <iostream>

static const int _stressTestVal = 10000;

void FAIL() {   exit(1);    }
void OK(const char* m) { std:: cout << "All test cases in test " << m << " are passed" << std:: endl;}
void t_true(bool p) { if (!p) FAIL(); }
void t_false(bool p) { if (p) FAIL(); }

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

        // Test removeString
        String* removedString = dynamic_cast<String*>(h1->remove(keyStr));
        t_false(h1->contains_key(keyStr));

        delete removedString;
    }

    OK("Stress test complete");
}

int main() {

    mapStressTest();
    return 0;
}


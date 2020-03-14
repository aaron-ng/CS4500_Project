#pragma once

#include <iostream>
#include <math.h>

void FAIL() { std::cerr << "Failure" << std::endl; exit(1); }
void OK(const char* m) { std::cout << "Success, Test: " << m << "\n"; }
void t_true(bool p) {
    if (!p) FAIL();
}
void t_false(bool p) {
    if (p) FAIL();
}

bool floatComparison(float a, float b, float epsilon = 0.05f) {
    return (fabs(a - b) < epsilon);
}
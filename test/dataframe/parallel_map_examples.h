#pragma once

#include <chrono>
#include <cmath>

#include "../../src/dataframe/dataframe.h"
#include "../test_util.h"

class ExampleOneRower : public Rower {
    public:
        size_t rows = 0;

        bool accept(Row &r) { r.set(1, r.get_int(0) * 2); rows++; return true; }
        virtual Object* clone() { return new ExampleOneRower(); }

        virtual void join_delete(Rower* other) {
            ExampleOneRower* rower = dynamic_cast<ExampleOneRower*>(other);
            if (rower) { rows += rower->rows; }
            delete other;
        }
};

class ExampleTwoRower : public Rower {
    public:
        bool accept(Row &r) {
            if (r.get_int(0) * 2 != r.get_int(1)) {
                std::cout << "Validation failure" << std::endl;
                exit(-1);
            }
            std::this_thread::sleep_for(std::chrono::nanoseconds (200));
            return true;
        }
        virtual Object* clone() { return new ExampleOneRower(); }
};

void populateDataFrame(DataFrame& df, size_t elements) {
    IntColumn source;
    IntColumn destination;

    for (size_t i = 0; i < elements; i++) {
        source.push_back(rand() % 10000);
        destination.push_back(0);
    }

    df.add_column(&source, nullptr);
    df.add_column(&destination, nullptr);
}

void example2(bool showTimes) {
    if (showTimes) {
        std::cout << "Running Pmap Example 2 (10000 elements)" << std::endl;
        std::cout << "This example doubles an integer and writes it to the adjacent column." << std::endl;
        std::cout << "The duplication is then verified with another map." << std::endl;
        std::cout << "The verifier is slow to make sure everything is correct (each element has a 200ns sleep after it is verified)" << std::endl;

    }
    Schema schema("");
    DataFrame df(schema);
    populateDataFrame(df, 10000);

    ExampleOneRower rower;
    ExampleTwoRower verifier;

    auto start = std::chrono::system_clock::now();
    df.map(rower);
    df.map(verifier);
    auto end = std::chrono::system_clock::now();
    if (showTimes) {
        std::cout << "Un-parallel plus verification version took "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                  << "ms"
                  << std::endl;
    }


    start = std::chrono::system_clock::now();
    df.pmap(rower);
    df.pmap(verifier);
    end = std::chrono::system_clock::now();
    if (showTimes) {
        std::cout << "parallel version took "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                  << "ms"
                  << std::endl;

        std::cout << "Verifying without parallelization one more time..." << std::endl;
    }

    // Perform one last, non-parallel verification to ensure that there is not a bug with the pmap
    df.map(verifier);

}

void example1(bool showTimes) {
    if (showTimes) {
        std::cout << "Running Pmap Example 1 (100000 elements)" << std::endl;
        std::cout << "This example doubles an integer and writes it to the adjacent column" << std::endl;
    }


    Schema schema("");

    for (size_t i = 0; i < 2; i++) {
        if (showTimes) {
            std::cout << "Running for " << 100000 * pow(10, i) << " rows" << std::endl;
        }

        DataFrame df(schema);
        populateDataFrame(df, 10000 * pow(10, i));

        ExampleOneRower rower;

        auto start = std::chrono::system_clock::now();
        df.map(rower);
        auto end = std::chrono::system_clock::now();
        if (showTimes) {
            std::cout << "Un-parallel version took "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                      << "ms for "
                      << rower.rows
                      << std::endl;
        }

        rower.rows = 0;
        start = std::chrono::system_clock::now();
        df.pmap(rower);
        end = std::chrono::system_clock::now();
        if (showTimes) {
            std::cout << "parallel version took "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                      << "ms for "
                      << rower.rows
                      << std::endl;
        }

    }
}

void testParallelMapExamples() {
    example1(false);
    example2(false);

    OK("ParallelMapExamples Passed");
}
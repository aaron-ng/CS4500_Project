#pragma once

#include <stdio.h>
#include <string.h>

/** The different actions that can be passed on the command line */
enum CMDAction: uint8_t {
    NONE,
    print_col_type,
    print_col_idx,
    is_missing_idx
};

/**
 * This macro will expand to parsing a single arg (such as -f) as well as capture any values associated with it.
 * It requires that i (int), argv (char**) and argc (int) exist in the current scope.
 *
 * On a successful match, i is incremented to skip all of the associated values and a continue is performed.
 *
 * @param arg The string representation of the argument to parse, for instance "-f"
 * @param requiredCount The number of required values after the argument is matched
 * @param success The code to be performed on a successful arg and value match. argv[i] will point to the first
 *        associated value when this code is performed.
 */
#define CMD_PARSE_ARG(arg, requiredCount, success) \
    if (!strcmp(argv[i], arg)) { \
        if (i + requiredCount < argc) { \
            i++; \
            success; \
            i += requiredCount; \
            continue; \
        } else { \
            std::cerr << arg << " requires " << requiredCount << " arguments\n"; \
            throw std::runtime_error("Missing args"); \
        } \
    }

/** Class that parses command line arguments */
class CMD {
    private:

        /** The filename to read from */
        const char* filename = nullptr;

        /** The row to perform an action on. -1 if not specified */
        long int row = -1;

        /** The column to perform an action on. -1 if not specified */
        long int col = -1;

        /** The offset from the start of the file that should be used */
        long int offset = 0;

        /** The length of the file that should be read. -1 if the entire file should be read */
        long int length = -1;

        /** The action that was requested from the command line */
        CMDAction action = NONE;

    public:

        /** Returns the name of the file to read from */
        const char* getFilename() const { return filename; }

        /** Returns the target row of the action. -1 if unspecified */
        long int getRow() const { return row; }

        /** Returns the target column of the action. -1 if unspecified */
        long int getCol() const { return col; }

        /** The offset from the beginning of the file to start reading from. 0 if unspecified */
        long int getOffset() const { return offset; }

        /** The number of bytes to read in the file from offset. -1 if unspecified */
        long int getLength() const { return length; }

        /** The action passed in the command line to be performed */
        CMDAction getAction() const { return action; }

        /**
         * Parses the command line arguments
         * @param argc The number of arguments in the value array
         * @param argv The contents of the arguments passed to the program
         */
        CMD(int argc, const char** argv) {
            for (int i = 0; i < argc;) {
                CMD_PARSE_ARG("-from", 1, offset = atoi(argv[i]));
                CMD_PARSE_ARG("-len", 1, length = atoi(argv[i]));

                CMD_PARSE_ARG("-f", 1, filename = argv[i]);
                CMD_PARSE_ARG("-print_col_type", 1, action = print_col_type; col = atoi(argv[i]));
                CMD_PARSE_ARG("-print_col_idx", 2, action = print_col_idx; col = atoi(argv[i]); row = atoi(argv[i + 1]));
                CMD_PARSE_ARG("-is_missing_idx", 2, action = is_missing_idx; col = atoi(argv[i]); row = atoi(argv[i + 1]));

                i++;
            }

            validateArgs();
        }

    private:

        /**
         * Validates the arguments passed in from the command line. If the args are invalid, an exception is thrown.
         */
        void validateArgs() {
            if (filename == nullptr) { std::cerr << "Please enter a filename \n"; throw std::runtime_error("Missing filename"); }
            if (action == NONE) { std::cerr << "Please enter an operation to perform \n"; throw std::runtime_error("Missing filename"); }
        }
};
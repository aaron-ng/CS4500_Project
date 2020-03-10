#pragma once

#include <stdio.h>

/** Set of utils for files */
class Files {
    public:

        /**
         * Provides the size in bytes of the given file
         * @param file The file to determine the size of
         * @return The size of the file in bytes
         */
        static size_t filesize(FILE* file) {
            size_t startPos = ftell(file);

            fseek(file, 0, SEEK_END);
            size_t length = ftell(file);

            fseek(file, startPos, SEEK_SET);
            return length;
        }

        /**
         * Seeks the file by the given offset. The file head will then be moved to the next character after a newline
         * since the line that was sought to could be incomplete.
         * @param file The file to seek
         * @param offset The offset in bytes to seek by. The actual number of bytes sought will either be greater than or
         *               equal to this value.
         * @return The number of bytes skipped to go to the next line
         */
        static size_t offsetBySkippingIncompleteLines(FILE* file, size_t offset) {
            fseek(file, offset, SEEK_CUR);

            char* line = nullptr;
            size_t length = 0;
            size_t skipped = getline(&line, &length, file);
            if (line) { free(line); }

            return skipped;
        }
};
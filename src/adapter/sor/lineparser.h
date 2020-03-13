#pragma once

#include <vector>

class LineParser {
    public:

        /**
         * Tokenizes a line using < and > as the starting token. If there are any duplicate < tokens, ie << "" >
         * the entire line will be ignored and empty will be returned. If there are any non-whitespace characters
         * outside of <>, the entire line will also be ignored.
         *
         * The returned tokens will not have < or > as well as any leading and trailing space between them
         * @param str The string to tokenize
         * @return The resulting tokenized and trimmed strings
         */
        std::vector<std::string> parseTokens(const std::string& str, size_t sizeHint = -1) const {
            std::vector<std::string> tokens;
            if (sizeHint != -1) { tokens.reserve(sizeHint); }

            size_t last_open = -1;
            bool in_quotes = false;
            for (int i = 0; i < str.length(); i++) {
                char atI = str[i];
                if (atI == '"') {
                    in_quotes = !in_quotes;
                } else if (!in_quotes) {
                    if (last_open == -1) {
                        if (atI == '<') { last_open = i; }
                        else if (!isspace(atI)) { return std::vector<std::string>(); }
                    } else {
                        if (atI == '<') { return std::vector<std::string>(); }
                        if (atI == '>') {
                            std::string token = stripDelimsAndWhitespace(str, last_open + 1, i - 1);
                            tokens.emplace_back(token);
                            last_open = -1;
                        }
                    }
                }
            }

            return tokens;
        }

    private:

        /**
         * Removes a leading and trailing < > as well as any leading or trailing whitespace in between
         * the brackets.
         * @param str The string to remove the unwanted characters from
         * @return The contents in between the <> with no leading or trailing whitespace
         */
        std::string stripDelimsAndWhitespace(const std::string& str, size_t start, size_t end) const {
            for (; start < end; start++) {
                if (str[start] != ' ') { break; }
            }

            for (; end >= start; end--) {
                if (str[end] != ' ') { break; }
            }

            return str.substr(start, end - start + 1);
        }
};
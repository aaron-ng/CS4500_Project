#pragma once

#include <iostream>

class Printer {
    public:

        /** Prints the string */
        virtual Printer& p(const char* contents) { 
            std::cout << contents;
            return *this; 
        }

        /** Prints the int */
        virtual Printer& p(int contents) { 
            std::cout << contents; 
            return *this;
        }
        
        /** Prints the float */
        virtual Printer& p(float contents) { 
            std::cout << contents;
            return *this;
        }

        /** Prints the bool */
        virtual Printer& p(bool contents) {
            std::cout << contents;
            return *this;
        }
        
        /** Prints the string with a newline after */
        virtual Printer& pl(const char* contents) { 
            std::cout << contents << std::endl;
            return *this; 
        }

        /** Prints the int with a newline after */
        virtual Printer& pl(int contents) { 
            std::cout << contents << std::endl;
            return *this; 
        }
        
        /** Prints the float with a newline after */
        virtual Printer& pl(float contents) { 
            std::cout << contents << std::endl;
            return *this; 
        }

        /** Prints the bool with a newline after */
        virtual Printer& pl(bool contents) { 
            std::cout << contents << std::endl;
            return *this; 
        }

};

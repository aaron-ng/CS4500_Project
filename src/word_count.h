#pragma once

#include "ea2/application.h"
#include "ea2/kvstore/kvstore.h"
#include "dataframe/dataframe.h"

class Num: public Object {
public:
    int v = 0;

    Num() {}
    Num(size_t v): v(v) {}
};


class KeyBuff: public Object {
public:
    Key* orig_;
    StrBuff buf_;

    KeyBuff(Key* orig) : orig_(orig), buf_() {}

    KeyBuff& c(String &s) { buf_.c(s); return *this;  }
    KeyBuff& c(size_t v) { buf_.c(v); return *this; }
    KeyBuff& c(const char* v) { buf_.c(v); return *this; }

    Key* get() {
        String* s = buf_.get();
        buf_.c(orig_->getName());
        Key* k = new Key(s->steal(), orig_->getNode());
        delete s;
        return k;
    }
};

/**
 *  CODE BELOW PROVIDED BY INSTRUCTORS AND MODIFIED FOR OUR IMPLEMENTATION
 */

class SIMap: public Map {
public:
    Map _map;
    /**
     * Wrapper method for the Map.contains_key method
     * @param key the key we are trying to find
     * @return a bool, true if the key does exist, false if not found
     */
    bool contains(Object* key) {
        return _map.contains_key(key);
    }

    /**
     * Wrapper method for the Map.contains_key method
     * @param key the key we are trying to find
     * @return a bool, true if the key does exist, false if not found
     */
    bool contains(String* key) {
        return _map.contains_key(key);
    }

    /**
     * Returns the number of key-value pairs in the map
     */
    int get_size() {
        return _map.get_size();
    }

};

class FileReader : public Writer {
public:
    /** Reads next word and stores it in the row. Actually read the word.
        While reading the word, we may have to re-fill the buffer  */
    void visit(Row & r) override {
        assert(i_ < end_);
        assert(! isspace(buf_[i_]));
        size_t wStart = i_;
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) { ++i_;  break; }
                i_ = wStart;
                wStart = 0;
                fillBuffer_();
            }
            if (isspace(buf_[i_]))  break;
            ++i_;
        }
        buf_[i_] = 0;
        String word(buf_ + wStart, i_ - wStart);
        r.set(0, word.clone());
        ++i_;
        skipWhitespace_();
    }

    /** Returns true when there are no more words to read.  There is nothing
       more to read if we are at the end of the buffer and the file has
       all been read.     */
    bool done() override { return (i_ >= end_) && feof(file_);  }

    /** Creates the reader and opens the file for reading.  */
    FileReader() {
        // file_ = fopen(arg.file, "r"); // Hardcoded
        file_ = fopen("../100k.txt", "r");
        if (file_ == nullptr) exit(15);
        buf_ = new char[BUFSIZE + 1]; //  null terminator
        fillBuffer_();
        skipWhitespace_();
    }

    static const size_t BUFSIZE = 1024;

    /** Reads more data from the file. */
    void fillBuffer_() {
        size_t start = 0;
        // compact unprocessed stream
        if (i_ != end_) {
            start = end_ - i_;
            memcpy(buf_, buf_ + i_, start);
        }
        // read more contents
        end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
        i_ = start;
    }

    /** Skips spaces.  Note that this may need to fill the buffer if the
        last character of the buffer is space itself.  */
    void skipWhitespace_() {
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) return;
                fillBuffer_();
            }
            // if the current character is not whitespace, we are done
            if (!isspace(buf_[i_]))
                return;
            // otherwise skip it
            ++i_;
        }
    }

    char * buf_;
    size_t end_ = 0;
    size_t i_ = 0;
    FILE * file_;
};


/****************************************************************************/
class Adder : public Reader {
public:
    SIMap& map_;  // String to Num map;  Num holds an int

    Adder(SIMap& map) : map_(map)  {}

    bool visit(Row& r) override {
        String* word = r.get_string(0);
        assert(word != nullptr);
        Num* num = map_.contains(word) ? dynamic_cast<Num*>(map_.get(word)) : new Num();
        assert(num != nullptr);
        num->v++;
        map_.put(word, num);
        return false;
    }
};

/***************************************************************************/
class Summer : public Writer {
public:
    SIMap& map_;
    size_t i = 0;
    size_t j = 0;
    size_t seen = 0;

    Summer(SIMap& map) : map_(map) {}

    void next() {
        if (i == map_.get_size() ) return;
        if (j < map_._map.entrySet().size()) {
            j++;
            ++seen;
        } else {
            ++i;
            j = 0;
            while( i < map_.get_size() && map_._map.entrySet().size() == 0 )  i++;
            if (k()) ++seen;
        }
    }

    String* k() {
        if (i==map_.get_size() || j == map_._map.entrySet().size()) return nullptr;
        return (String*) (map_._map.entrySet()[j]);
    }

    size_t v() {
        if (i == map_.get_size() || j == map_._map.entrySet().size()) {
            assert(false); return 0;
        }
        return ((Num*)(map_._map.entrySet()[j]))->v;
    }

    void visit(Row& r) {
        if (!k()) next();
        String & key = *k();
        size_t value = v();
        r.set(0, &key);
        r.set(1, (int) value);
        next();
    }

    bool done() {return seen == map_.get_size(); }
};

/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount: public Application {
public:
    static const size_t BUFSIZE = 1024;
    Key in;
    KeyBuff kbuf;
    SIMap all;

    WordCount(size_t idx, KVStore& kvStore):
            Application(idx, kvStore), in("data"), kbuf(new Key("wc-map-",0)) { }

    /** The master nodes reads the input, then all of the nodes count. */
    void _run() override {
        if (this_node() == 0) {
            FileReader fr;
            DataFrame::fromVisitor(&in, &kv, "S", &fr);
        }
        local_count();
        reduce();
    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key* mk_key(size_t idx) {
        Key * k = kbuf.c(idx).get();
//        LOG("Created key " << k->c_str());
        return k;
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
        DataFrame* words = (kv.waitAndGet(in));
        p("Node ").p(this_node()).pln(": starting local count...");
        SIMap map;
        Adder add(map);
        words->local_map(add);
        delete words;
        Summer cnt(map);
        DataFrame::fromVisitor(mk_key(this_node()), &kv, "SI", &cnt);
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (this_node() != 0) return;
        pln("Node 0: reducing counts...");
        SIMap map;
        Key* own = mk_key(0);
        merge(kv.get(*own), map);
        for (size_t i = 1; i < kv._byteStore.nodes(); ++i) { // merge other nodes
            Key* ok = mk_key(i);
            merge(kv.waitAndGet(*ok), map);
            delete ok;
        }
        p("Different words: ").pln(map.get_size());
        delete own;
    }

    void merge(DataFrame* df, SIMap& m) {
        Adder add(m);
        df->map(add);
        delete df;
    }
}; // WordcountDemo
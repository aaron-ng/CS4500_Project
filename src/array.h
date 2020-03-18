//lang::CwC 
#pragma once

#include <assert.h>
#include <cstring>

#include "string.h"
#include "object.h"
#include "element_column.h"

/**
 * A resizable array that is composed of Elements
 */
class RawArray {
    public:

        /** The storage for all of the elements in the array */
        Element* _storage = new Element[1];

        /** The capacity of _storage */
        size_t _capacity = 1;

        /** The number of items in the array */
        size_t _length = 0;

        /**
         * Constructor.
        */
        RawArray() {}

        /**
         * Reallocates the buffer to be of size _capacity and then copies all of the elements currently in the array
         * to the new buffer.
         */
        void _reallocateStorage() {
            Element* newStorage = new Element[_capacity];
            memcpy(newStorage, _storage, sizeof(Element) * _length);

            delete [] _storage;
            _storage = newStorage;
        }

        /**
         * Doubles the size of the array if it is full
         */
        void _expandIfNeeded() {
            if (_length == _capacity) {
                _capacity *= 2;
                _reallocateStorage();
            }
        }

        /**
         * Appends e to end of the list.
         * @arg e: the Object to be appended.
        */
        virtual void push_back(Element e) {
            _expandIfNeeded();
            _storage[_length] = e;
            _length += 1;
        }

        /**
         * Inserts e at i. All Objects from i right, are moved a position to the right.
         * @arg i: the index where you want to add the Object.
         * @arg e: the Object to be appended.
        */
        virtual void add(size_t i, Element e) {
            assert(i >= 0 && i <= _length);
            _expandIfNeeded();

            if (i != _length) {
                for (size_t index = _length; index > i; index--) {
                    _storage[index] = _storage[index - 1];
                }
            }

            _storage[i] = e;
            _length += 1;
        }

        /**
         * Inserts all of elements in c into this list at i
         * @arg i: the index starting where you want to add the string.
         * @arg e: the List of Objects to be appended.
        */
        virtual void add_all(size_t i, RawArray* c) {
            assert(i >= 0 && i <= _length);
            size_t requiredCapacity = _length + c->_length;
            if (requiredCapacity > _capacity) {
                _capacity = requiredCapacity;
                _reallocateStorage();
            }

            if (i != _length) {
                for (size_t index = _length - 1; index <= i; index--) {
                    _storage[index + c->_length] = _storage[index];
                }
            }

            for (size_t index = i; index < c->_length; index++) {
                _storage[i + index] = c->_storage[index];
            }

            _length += c->_length;
        }

        /**
         * Removes all of elements from this list
        */
        virtual void clear() { _length = 0; }

        /**
         * Returns the element at index
         * arg index: the index of the element that you want.
        */
        virtual Element get(size_t index) {
            assert(index >= 0 && index < _length);
            return _storage[index];
        }

        /**
         * Removes the element at i
         * arg i: the index of the element you want to remove.
        */
        virtual Element remove(size_t i) {
            assert(i >= 0 && i < _length);
            Element removed = _storage[i];

            for (size_t index = i; index < _length - 1; index++) {
                _storage[index] = _storage[index + 1];
            }
            _length -= 1;
            return removed;
        }

        /**
         * Replaces the element at i with e
         * arg i: the index of the element you want to replace.
         * arg e: the element that you're replacing it with.
        */
        virtual Element set(size_t i, Element o) {
            assert(i >= 0 && i < _length);
            Element replacing = _storage[i];
            _storage[i] = o;

            return replacing;
        }

        /**
         * Return the number of elements in the collection
        */
        virtual size_t size() { return _length; }

        /**
         * Destructor.
        */
        virtual ~RawArray() {
            delete [] _storage;
        }
};

/*
* ArrayObject: This class represents a list of Objects.
* author: shetty.y@husky.neu.edu, eldrid.s@husky.neu.edu
*/
class ArrayObject : public Object {
public:

    /** The raw array that backs this array */
    RawArray _rawArray;

    /**
	 * Constructor.
	*/
    ArrayObject() {}


    /**
     * Appends e to end of the list.
     * @arg e: the Object to be appended.
    */
    virtual void push_back(Object* e) {
        Element element;
        element.ptr = e;
        _rawArray.push_back(element);
    }

    /**
     * Inserts e at i. All Objects from i right, are moved a position to the right.
     * @arg i: the index where you want to add the Object.
     * @arg e: the Object to be appended.
    */
    virtual void add(size_t i, Object* e) {
        Element element;
        element.ptr = e;
        _rawArray.add(i, element);
    }
    
    /**
     * Inserts all of elements in c into this list at i
     * @arg i: the index starting where you want to add the string.
     * @arg e: the List of Objects to be appended.
    */
    virtual void add_all(size_t i, ArrayObject* c) {
        _rawArray.add_all(i, &c->_rawArray);
    }

    /**
     * Removes all of elements from this list
    */
    virtual void clear() { return _rawArray.clear(); }

    /**
     * Compares o with this list for equality.
     * arg o: the Object you're testing equality against.
    */
    virtual bool equals(Object* o) {
        ArrayObject* array = dynamic_cast<ArrayObject*>(o);
        if (array && array->size() == size()) {
            for (size_t i = 0; i < size(); i++) {
                Object* selfElement = reinterpret_cast<Object*>(_rawArray.get(i).ptr);
                Object* otherElement = reinterpret_cast<Object*>(_rawArray.get(i).ptr);

                if (!selfElement->equals(otherElement)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the element at index
     * arg index: the index of the element that you want.
    */
    virtual Object* get(size_t index) {
        return reinterpret_cast<Object*>(_rawArray.get(index).ptr);
    }

    /**
     * Returns the hash code value for this list.
    */
    virtual size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < _rawArray.size(); i++) {
            hash += reinterpret_cast<Object*>(_rawArray.get(i).ptr)->hash();
        }

        return hash;
    }

    /**
     * Returns the index of the first occurrence of o, or >size() if not there
     * arg o: the Object that you want the index for.
    */
    virtual size_t index_of(Object* o) {
        for (size_t index = 0; index < _rawArray.size(); index++) {
            if (o->equals(reinterpret_cast<Object*>(_rawArray.get(index).ptr))) { return index; }
        }
        return size() + 1;
    }

    /**
     * Removes the element at i
     * arg i: the index of the element you want to remove. 
    */
    virtual Object* remove(size_t i) {
        Element e = _rawArray.remove(i);
        return reinterpret_cast<Object*>(e.ptr);
    }

    /**
     * Replaces the element at i with e
     * arg i: the index of the element you want to replace. 
     * arg e: the element that you're replacing it with.
    */
    virtual Object* set(size_t i, Object* o) {
        Element e;
        e.ptr = o;
        return reinterpret_cast<Object*>(_rawArray.set(i, e).ptr);
    }

    /**
     * Return the number of elements in the collection
    */
    virtual size_t size() { return _rawArray.size(); }

    /**
     * Destructor.
    */
    virtual ~ArrayObject() {}

    /**
     * Tests whether this list contains an Object, with the same value as e.
     * @arg e: The Object you want to test for containment.
    */
    virtual bool contains(Object* e) {
        return index_of(e) <= size();
    }
};


/*
* ArrayString: This class represents a list of String objects.
* author: shetty.y@husky.neu.edu, eldrid.s@husky.neu.edu
*/
class ArrayString : public Object {
public:

        /** The raw array that backs this array */
        ArrayObject _array;

    /**
	 * Constructor.
	*/
    ArrayString() {
    }

    /**
     * Appends e to end of the list.
     * @arg e: the String to be appended.
    */
    virtual void push_back(String* e) {
        _array.push_back(e);
    }

    /**
     * Inserts e at i. All strings from i right, are moved a position to the right.
     * @arg i: the index where you want to add the string.
     * @arg e: the String to be appended.
    */
    virtual void add(size_t i, String* e) {
        _array.add(i, e);
    }
    
    /**
     * Inserts all of elements in c into this list at i
     * @arg i: the index starting where you want to add the string.
     * @arg e: the List of Strings to be appended.
    */
    virtual void add_all(size_t i, ArrayString* c) {
        _array.add_all(i, &c->_array);
    }

    /**
     * Removes all of elements from this list
    */
    virtual void clear() {
        _array.clear();
    }

    /**
     * Compares o with this list for equality.
     * arg o: the Object you're testing equality against.
    */
    virtual bool equals(Object* o) {
        ArrayString* other = dynamic_cast<ArrayString*>(o);
        return other ? _array.equals(&other->_array) : false;
    }

    /**
     * Returns the element at index
     * arg index: the index of the element that you want.
    */
    virtual String* get(size_t index) {
        return dynamic_cast<String*>(_array.get(index));
    }

    /**
     * Returns the hash code value for this list.
    */
    virtual size_t hash() {
        return _array.hash();
    }

    /**
     * Returns the index of the first occurrence of o, or >size() if not there
     * arg o: the Object that you want the index for.
    */
    virtual size_t index_of(Object* o) {
        return _array.index_of(o);
    }

    /**
     * Removes the element at i
     * arg i: the index of the element you want to remove. 
    */
    virtual String* remove(size_t i) {
        return dynamic_cast<String*>(_array.remove(i));
    }

    /**
     * Replaces the element at i with e
     * arg i: the index of the element you want to replace. 
     * arg e: the element that you're replacing it with.
    */
    virtual String* set(size_t i, String* e) {
        return dynamic_cast<String*>(_array.set(i, e));
    }

    /**
     * Return the number of elements in the collection
    */
    virtual size_t size() {
        return _array.size();
    }

    /**
     * Destructor.
    */
    virtual ~ArrayString() {}

    /**
     * Tests whether this list contains a String, with the same value as e.
     * @arg e: The string you want to test for containment.
    */
    virtual bool contains(String* e) {
        return _array.contains(e);
    }
};

class ArrayInt : public Object {
public:

    /** The array that actually stores the elements */
    RawArray _rawArray;

    /**
     * Constructor.
    */
    ArrayInt() {
    }

    /**
     * Appends e to end of the list.
     * @arg e: the int to be appended.
    */
    virtual void push_back(int e) {
        Element element;
        element.i = e;
        _rawArray.push_back(element);
    }

    /**
     * Inserts e at i. All Objects from i right, are moved a position to the right.
     * @arg i: the index where you want to add the int.
     * @arg e: the int to be appended.
    */
    virtual void add(size_t i, int e) {
        Element element;
        element.i = e;
        _rawArray.add(i, element);
    }
    
    /**
     * Inserts all of elements in c into this list at i
     * @arg i: the index starting where you want to add the string.
     * @arg e: the List of Objects to be appended.
    */
    virtual void add_all(size_t i, ArrayInt* e) {
        _rawArray.add_all(i, &e->_rawArray);
    }

    /**
     * Removes all of elements from this list
    */
    virtual void clear() {
        _rawArray.clear();
    }

    /**
     * Compares o with this list for equality.
     * arg o: the Object you're testing equality against.
    */
    virtual bool equals(Object* o) {
        ArrayInt* array = dynamic_cast<ArrayInt*>(o);
        if (array && array->size() == size()) {
            for (size_t i = 0; i < size(); i++) {
                if (_rawArray.get(i).i != array->_rawArray.get(i).i) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the element at index
     * arg index: the index of the element that you want.
    */
    virtual int get(size_t index) {
        return _rawArray.get(index).i;
    }

    /**
     * Returns the hash code value for this list.
    */
    virtual size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < _rawArray.size(); i++) {
            hash += _rawArray.get(i).i;
        }
        return hash;
    }

    /**
     * Returns the index of the first occurrence of o, or >size() if not there
     * arg i: the int that you want the index for.
    */
    virtual size_t index_of(int i) {
        for (size_t index = 0; index < _rawArray.size(); index++) {
            if (i == _rawArray.get(index).i) { return index; }
        }
        return size() + 1;
    }

    /**
     * Removes the element at i
     * arg i: the index of the element you want to remove. 
    */
    virtual int remove(size_t i) {
        return _rawArray.remove(i).i;
    }

    /**
     * Replaces the element at i with e
     * arg i: the index of the element you want to replace. 
     * arg e: the element that you're replacing it with.
    */
    virtual int set(size_t i, int e) {
        Element element;
        element.i = e;
        return _rawArray.set(i, element).i;
    }

    /**
     * Return the number of elements in the collection
    */
    virtual size_t size() {
        return _rawArray.size();
    }

    /**
     * Destructor.
    */
    virtual ~ArrayInt() {
    } 

    /**
     * Tests whether this list contains an int, with the same value as e.
     * @arg e: The int you want to test for containment.
    */
    virtual bool contains(int e) {
        return index_of(e) <= size();
    }
};

class ArrayFloat : public Object {
public:

        /** The raw array that backs this array */
        RawArray _rawArray;

    /**
     * Constructor.
    */
    ArrayFloat() {
    }

    /**
     * Appends e to end of the list.
     * @arg e: the float to be appended.
    */
    virtual void push_back(float e) {
        Element element;
        element.f = e;
        _rawArray.push_back(element);
    }

    /**
     * Inserts e at i. All Objects from i right, are moved a position to the right.
     * @arg i: the index where you want to add the float.
     * @arg e: the float to be appended.
    */
    virtual void add(size_t i, float e) {
        Element element;
        element.f = e;
        _rawArray.add(i, element);
    }
    
    /**
     * Inserts all of elements in c into this list at i
     * @arg i: the index starting where you want to add the string.
     * @arg e: the List of floats to be appended.
    */
    virtual void add_all(size_t i, ArrayFloat* c) {
        _rawArray.add_all(i, &c->_rawArray);
    }

    /**
     * Removes all of elements from this list
    */
    virtual void clear() {
        _rawArray.clear();
    }

    /**
     * Compares o with this list for equality.
     * arg o: the Object you're testing equality against.
    */
    virtual bool equals(Object* o) {
        ArrayFloat* array = dynamic_cast<ArrayFloat*>(o);
        if (array && array->size() == size()) {
            for (size_t i = 0; i < size(); i++) {
                if (_rawArray.get(i).f != array->_rawArray.get(i).f) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the element at index
     * arg index: the index of the element that you want.
    */
    virtual float get(size_t index) {
        return _rawArray.get(index).f;
    }

    /**
     * Returns the hash code value for this list.
    */
    virtual size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < _rawArray.size(); i++) {
            hash += _rawArray.get(i).f;
        }
        return hash;
    }

    /**
     * Returns the index of the first occurrence of o, or >size() if not there
     * arg o: the Object that you want the index for.
    */
    virtual size_t index_of(float o) {
        for (size_t index = 0; index < _rawArray.size(); index++) {
            if (o == _rawArray.get(index).f) { return index; }
        }
        return size() + 1;
    }

    /**
     * Removes the element at i
     * arg i: the index of the element you want to remove. 
    */
    virtual float remove(size_t i) {
        return _rawArray.remove(i).f;
    }

    /**
     * Replaces the element at i with e
     * arg i: the index of the element you want to replace. 
     * arg e: the element that you're replacing it with.
    */
    virtual float set(size_t i, float o) {
        Element element;
        element.f = o;
        return _rawArray.set(i, element).f;
    }

    /**
     * Return the number of elements in the collection
    */
    virtual size_t size() {
        return _rawArray.size();
    }

    /**
     * Destructor.
    */
    virtual ~ArrayFloat() {
    } 

    /**
     * Tests whether this list contains an float, with the same value as e.
     * @arg e: The float you want to test for containment.
    */
    virtual bool contains(float e) {
         return index_of(e) <= size();
    }
};

class ArrayBool : public Object {
public:

        /** The raw array that backs this array */
        RawArray _rawArray;

    /**
     * Constructor.
    */
    ArrayBool() {
    }

    /**
     * Appends e to end of the list.
     * @arg e: the bool to be appended.
    */
    virtual void push_back(bool e) {
        Element element;
        element.b = e;
        _rawArray.push_back(element);
    }

    /**
     * Inserts e at i. All Objects from i right, are moved a position to the right.
     * @arg i: the index where you want to add the bool.
     * @arg e: the bool to be appended.
    */
    virtual void add(size_t i, bool e) {
        Element element;
        element.b = e;
        _rawArray.add(i, element);
    }
    
    /**
     * Inserts all of elements in c into this list at i
     * @arg i: the index starting where you want to add the string.
     * @arg e: the List of bools to be appended.
    */
    virtual void add_all(size_t i, ArrayBool* c) {
        _rawArray.add_all(i, &c->_rawArray);
    }

    /**
     * Removes all of elements from this list
    */
    virtual void clear() {
        _rawArray.clear();
    }

    /**
     * Compares o with this list for equality.
     * arg o: the Object you're testing equality against.
    */
    virtual bool equals(Object* o) {
        ArrayBool* array = dynamic_cast<ArrayBool*>(o);
        if (array && array->size() == size()) {
            for (size_t i = 0; i < size(); i++) {
                if (_rawArray.get(i).b != array->_rawArray.get(i).b) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns the element at index
     * arg index: the index of the element that you want.
    */
    virtual bool get(size_t index) {
        return _rawArray.get(index).b;
    }

    /**
     * Returns the hash code value for this list.
    */
    virtual size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < _rawArray.size(); i++) {
            hash += _rawArray.get(i).b;
        }
        return hash;
    }

    /**
     * Returns the index of the first occurrence of o, or >size() if not there
     * arg o: the bool that you want the index for.
    */
    virtual size_t index_of(bool o) {
        for (size_t index = 0; index < _rawArray.size(); index++) {
            if (o == _rawArray.get(index).b) { return index; }
        }
        return size() + 1;
    }

    /**
     * Removes the element at i
     * arg i: the index of the element you want to remove. 
    */
    virtual bool remove(size_t i) {
        return _rawArray.remove(i).b;
    }

    /**
     * Replaces the element at i with e
     * arg i: the index of the element you want to replace. 
     * arg e: the element that you're replacing it with.
    */
    virtual bool set(size_t i, bool o) {
        Element element;
        element.b = o;
        return _rawArray.set(i, element).b;
    }

    /**
     * Return the number of elements in the collection
    */
    virtual size_t size() {
        return _rawArray.size();
    }

    /**
     * Destructor.
    */
    virtual ~ArrayBool() {
    } 

    /**
     * Tests whether this list contains an bool, with the same value as e.
     * @arg e: The bool you want to test for containment.
    */
    virtual bool contains(bool e) {
        return index_of(e) <= size();
    }
};
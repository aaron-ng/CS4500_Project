compile:
	- mkdir build
	cd build && cmake .. && make

test: compile
	./build/tests &> /dev/null; if [ $$? -eq 0 ] ; then echo "Tests passed"; else echo "Tests failed"; fi

valgrind: compile
	valgrind --leak-check=yes ./build/main

clean:
	rm -rf -f build

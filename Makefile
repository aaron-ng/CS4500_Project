compile:
	- mkdir build
	cd build && cmake .. && make

test: compile
	./build/main

valgrind: compile
	valgrind ./build/main

clean:
	rm -rf -f build
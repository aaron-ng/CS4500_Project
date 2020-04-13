compile:
	- mkdir build
	cd build && cmake .. && make

test: compile
	./build/tests > /dev/null; if [ $$? -eq 0 ] ; then echo "Tests passed"; else echo "Tests failed"; fi

run: compile
	./build/server &
	sleep 2s
	./build/main 127.0.0.1 25565 &
	./build/main 127.0.0.1 25566 &
	./build/main 127.0.0.1 25567

valgrind: compile
	./build/server &
	sleep 2s
	./build/main 127.0.0.1 25565 &
	./build/main 127.0.0.1 25566 &

	# A leak is supressed here that has to do with the specific implementation of threads. More info
	# can be found here https://stackoverflow.com/questions/57016280/memory-leaks-in-pthread-even-if-the-state-is-detached.
	# This doesn't supress any other leaks.
	valgrind --leak-check=full --suppressions=valgrind_sup.txt ./build/main 127.0.0.1 25567

clean:
	rm -rf -f build

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
	./build/main localhost 25565 &
	./build/main localhost 25566 &
	valgrind --leak-check=yes ./build/main localhost 25567

clean:
	rm -rf -f build

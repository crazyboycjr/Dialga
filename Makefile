# make clean; make for non-GDR version
# make clean; GDR=1 make for GDR version
all = server client
target = server.o client.o
objects = kvstore-rdma.o rdmatools.o
headers = config.hpp kvstore-rdma.hpp rdmatools.hpp
CC = g++

CFLAGS = -O3 -Wall
LDFLAGS = -libverbs -lglog -lpthread -lgflags
all: $(all)

server: $(objects)
	$(CC) -o server server.cpp $(objects) $(LDFLAGS)

client: $(objects)
	$(CC) -o client client.cpp $(objects) $(LDFLAGS)

$(objects) : %.o : %.cpp $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY : clean
clean:
	rm $(all)  $(objects)

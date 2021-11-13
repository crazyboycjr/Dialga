# make clean; make for non-GDR version
# make clean; GDR=1 make for GDR version
all = server client
target = server.o client.o
objects = kvstore-rdma.o rdmatools.o
headers = config.hpp kvstore-rdma.hpp rdmatools.hpp
CC = g++

CFLAGS = -Wall -g
LDFLAGS = -libverbs -lglog -lpthread -lgflags
all: $(all)

server: server.o $(objects)
	$(CC) -o server server.cpp $(objects) $(LDFLAGS) $(CFLAGS)

client: client.o $(objects)
	$(CC) -o client client.cpp $(objects) $(LDFLAGS) $(CFLAGS)

$(objects) : %.o : %.cpp $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY : clean
clean:
	rm $(all)  $(objects)

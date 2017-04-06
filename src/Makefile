all: loadbalancer loadbalancer.o client csapp.o client.o

loadbalancer : loadbalancer.o csapp.o 
	gcc -o loadbalancer loadbalancer.o csapp.o -lpthread

client: client.o csapp.o
	gcc -o client client.o csapp.o -lpthread

loadbalancer.o: loadbalancer.c
	gcc -c loadbalancer.c 

client.o: client.c
	gcc -c client.c

csapp.o: csapp.c
	gcc -c csapp.c

clean:	
	rm loadbalancer.o csapp.o client 


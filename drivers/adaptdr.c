#include "adaptdr.h"

int sendexact(int s, char *buf, int len)
{	// code to be sure to send all the data in buf up to length len
	// orig from: http://beej.us/guide/bgnet/output/html/multipage/advanced.html#sendall
	int total = 0;        // how many bytes we've sent
    int bytesleft = len; // how many we have left to send
    int n = -1;

    while(total < len) {
        n = send(s, buf+total, bytesleft, 0);
        if (n == -1) { break; }
        total += n;
        bytesleft -= n;
    }

    len = total; // return number actually sent here

    return n==-1?-1:0; // return -1 on failure, 0 on success
}

int recvexact(int s, char *buf, int len)
{   // code to be sure to recv all the data in buf up to length len
	// orig from: http://beej.us/guide/bgnet/output/html/multipage/advanced.html#sendall
    int total = 0;        // how many bytes we've sent
    int bytesleft = len; // how many we have left to send
    int n = -1;

    while(total < len) {
        n = recv(s, buf+total, bytesleft, 0);
        if (n == -1) { break; }
        total += n;
        bytesleft -= n;
    }

    len = total; // return number actually sent here

    return n==-1?-1:0; // return -1 on failure, 0 on success
}


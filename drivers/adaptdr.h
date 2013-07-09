#ifndef ADAPTDR_H
#define ADAPTDR_H

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#define REQ_INFO_SIZE 24
/* Size of req_info struct -- needs to be hardcoded since used to determine array size in
 * block-asyncdr.c
 */
/* !TW! Circular buffer for holding requests waiting to be asyncly sent */
#define DRBUFSIZE 10000
#define DR_MAX_WRITE_SIZE 1024*4
// buffer can hold DRBUFSIZE disk requests


#define ACK_PORT 9990
// DR_PORT is specified in config file

/* !TW! MUST KEEP IN SYNC WITH backupServer.c / block-adpatdr.c */
struct req_info {
	uint64_t writeID;
	int size;
	uint64_t offset;
	char* dataPtr;	// NOT USED /////
};

struct dr_ack {
	int deviceID;
	uint64_t writeID;

};

static int sendexact(int s, char *buf, int len)
{	// code to be sure to send all the data in buf up to length len
	// orig from: http://beej.us/guide/bgnet/output/html/multipage/advanced.html#sendall
	int total = 0;        // how many bytes we've sent
    int bytesleft = len; // how many we have left to send
    int n;

    while(total < len) {
        n = send(s, buf+total, bytesleft, 0);
        if (n == -1) { break; }
        total += n;
        bytesleft -= n;
    }

    len = total; // return number actually sent here

    return n==-1?-1:0; // return -1 on failure, 0 on success
}

static int recvexact(int s, char *buf, int len)
{   // code to be sure to recv all the data in buf up to length len
	// orig from: http://beej.us/guide/bgnet/output/html/multipage/advanced.html#sendall
    int total = 0;        // how many bytes we've sent
    int bytesleft = len; // how many we have left to send
    int n;

    while(total < len) {
        n = recv(s, buf+total, bytesleft, 0);
        if (n == -1) { break; }
        total += n;
        bytesleft -= n;
    }

    len = total; // return number actually sent here

    return n==-1?-1:0; // return -1 on failure, 0 on success
}


#endif

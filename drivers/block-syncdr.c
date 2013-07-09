/*
 * Copyright (c) 2007, XenSource Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of XenSource Inc. nor the names of its contributors
 *       may be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

// !TW! adding networking libs
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "tapdisk.h"
#include "tapdisk-driver.h"
#include "tapdisk-interface.h"

#include "adaptdr.h"

#define MAX_AIO_REQS         TAPDISK_DATA_REQUESTS

struct tdsyncdr_state;

struct syncdr_request {
	td_request_t         treq;
	struct tiocb         tiocb;
	struct tdsyncdr_state  *state;
};

struct tdsyncdr_state {
	int                  fd;
	td_driver_t         *driver;

	int                  syncdr_free_count;
	struct syncdr_request   syncdr_requests[MAX_AIO_REQS];
	struct syncdr_request  *syncdr_free_list[MAX_AIO_REQS];

	uint64_t pendingWrite;	// epoch of last started write (get from kblock?)
	uint64_t committedWrite; // epoch of last committed write
	int backupSocket;
	//int backupSocketId;	// was going to use this for event handler stuff

	// backup server info
	char* backupHost;
	int backupPort;
	char* imageFile;

};



/*Get Image size, secsize*/
static int tdsyncdr_get_image_info(int fd, td_disk_info_t *info)
{
	int ret;
	unsigned long long bytes;
	struct stat stat;

	ret = fstat(fd, &stat);
	if (ret != 0) {
		DPRINTF("ERROR: fstat failed, Couldn't stat image");
		return -EINVAL;
	}

	if (S_ISBLK(stat.st_mode)) {
		/*Accessing block device directly*/
		info->size = 0;
		if (ioctl(fd,BLKGETSIZE64,&bytes)==0) {
			info->size = bytes >> SECTOR_SHIFT;
		} else if (ioctl(fd,BLKGETSIZE,&info->size)!=0) {
			DPRINTF("ERR: BLKGETSIZE and BLKGETSIZE64 failed, couldn't stat image");
			return -EINVAL;
		}

		DPRINTF("Image size: \n\tpre sector_shift  [%llu]\n\tpost "
			"sector_shift [%llu]\n",
			(long long unsigned)(info->size << SECTOR_SHIFT),
			(long long unsigned)info->size);

		/*Get the sector size*/
#if defined(BLKSSZGET)
		{
			info->sector_size = DEFAULT_SECTOR_SIZE;
			ioctl(fd, BLKSSZGET, &info->sector_size);
			
			if (info->sector_size != DEFAULT_SECTOR_SIZE)
				DPRINTF("Note: sector size is %ld (not %d)\n",
					info->sector_size, DEFAULT_SECTOR_SIZE);
		}
#else
		info->sector_size = DEFAULT_SECTOR_SIZE;
#endif

	} else {
		/*Local file? try fstat instead*/
		info->size = (stat.st_size >> SECTOR_SHIFT);
		info->sector_size = DEFAULT_SECTOR_SIZE;
		DPRINTF("Image size: \n\tpre sector_shift  [%llu]\n\tpost "
			"sector_shift [%llu]\n",
			(long long unsigned)(info->size << SECTOR_SHIFT),
			(long long unsigned)info->size);
	}

	if (info->size == 0) {		
		info->size =((uint64_t) 16836057);
		info->sector_size = DEFAULT_SECTOR_SIZE;
	}
	info->info = 0;

	return 0;
}

/* Find out the server name, port, and disk image file
 *
 *  name has format = obelix29:9000:/home/twood/vms/testdisk.img
 *
 * */
int tdsyncdr_get_args(td_driver_t *driver, const char* name)
{
	struct tdsyncdr_state *state = (struct tdsyncdr_state *)driver->data;
	char* port;
	char* file;
	char* seperator;
	int portnum;

	seperator = strchr(name, ':');
	if (!seperator) {
		DPRINTF("missing host in %s\n", name);
		return -ENOENT;
	}
	if (!(state->backupHost = strndup(name, seperator - name))) {
		DPRINTF("unable to allocate host\n");
		return -ENOMEM;
	}
	seperator++; // move past ":"
	port = seperator; // start of port characters
	seperator = strchr(port, ':'); // end of port chars
	if (!seperator) {
		DPRINTF("missing port in %s\n", port);
		return -ENOENT;
	}
	if (!(port = strndup(port, seperator - port))) {
		DPRINTF("unable to allocate port\n");
		return -ENOMEM;
	}
	portnum = atoi(port);

	seperator++; // move past ":"
	file = seperator; // start of file name, ended by string terminator
	if (!(state->imageFile = strdup(file))) {
		DPRINTF("unable to allocate image path\n");
		return -ENOMEM;
	}

	state->backupPort = portnum;

	DPRINTF("host: %s, port: %d, image path: %s\n",
			state->backupHost, state->backupPort, state->imageFile);

	// TODO: free unused memory????

	return 0;
}

int tdsyncdr_connectTobackup(struct tdsyncdr_state *state) {
	int portno, n;
	struct sockaddr_in serv_addr;
	struct hostent *server;
	char buffer[256];


	state->backupSocket = socket(AF_INET, SOCK_STREAM, 0);
	if (state->backupSocket < 0) {
		DPRINTF("ERROR opening socket");
		return -1;
	}

	portno = state->backupPort;
	server = gethostbyname(state->backupHost);

	if (server == NULL) {
		DPRINTF("no such host");
		return -1;
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr,
		 (char *)&serv_addr.sin_addr.s_addr,
		 server->h_length);
	serv_addr.sin_port = htons(portno);
	if (connect(state->backupSocket,&serv_addr,sizeof(serv_addr)) < 0){
		DPRINTF("ERROR CONNECTING");
		return -1;
	}

	bzero(buffer,256);
	strcpy(buffer,state->imageFile);

	n = write(state->backupSocket,buffer,strlen(buffer));
	if (n < 0) {
		DPRINTF("ERROR writing to socket");
		return -1;
	}
	bzero(buffer,256);
	n = read(state->backupSocket,buffer,255);
	if (n < 0) {
		DPRINTF("ERROR reading from socket");
		return -1;
	}

	DPRINTF("read: %s", buffer);
/*
 * UNfinished attempt at registering event handlers?????
	if((state->backupSocketId = tapdisk_server_register_event(SCHEDULER_POLL_READ_FD, state->backupSocket, 0, eventRecvFromBackup, state)) < 0) {
		DPRINTF("error registering client event handler: %s\n", strerror(state->backupSocketId));
		//close(fd);
		return -1;
	}
*/

	return 0;
}


/* Open the disk file and initialize syncdr state. */
int tdsyncdr_open(td_driver_t *driver, const char *name, td_flag_t flags)
{
	int i, fd, ret, o_flags;
	struct tdsyncdr_state *prv;

	ret = 0;
	prv = (struct tdsyncdr_state *)driver->data;

	DPRINTF("block-syncdr open('%s')", name);

	memset(prv, 0, sizeof(struct tdsyncdr_state));

	prv->syncdr_free_count = MAX_AIO_REQS;
	for (i = 0; i < MAX_AIO_REQS; i++)
		prv->syncdr_free_list[i] = &prv->syncdr_requests[i];

	ret = tdsyncdr_get_args(driver, name);
	if(ret) {
		DPRINTF("ERROR: in get_args\n");
		goto done;
	}

	/* Open the file */
	o_flags = O_DIRECT | O_LARGEFILE |
		((flags & TD_OPEN_RDONLY) ? O_RDONLY : O_RDWR);
        fd = open(prv->imageFile, o_flags);

        if ( (fd == -1) && (errno == EINVAL) ) {

                /* Maybe O_DIRECT isn't supported. */
		o_flags &= ~O_DIRECT;
                fd = open(prv->imageFile, o_flags);
                if (fd != -1) DPRINTF("WARNING: Accessing image without"
                                     "O_DIRECT! (%s)\n", prv->imageFile);

        } else if (fd != -1) DPRINTF("open(%s) with O_DIRECT\n", prv->imageFile);

        if (fd == -1) {
		DPRINTF("Unable to open [%s] (%d)!\n", prv->imageFile, 0 - errno);
        	ret = 0 - errno;
        	goto done;
        }

	ret = tdsyncdr_get_image_info(fd, &driver->info);

	/* Setup state for network/backup server */
	prv->pendingWrite = 0;
	prv->committedWrite = 0;


	DPRINTF("Connecting to backup...");
	tdsyncdr_connectTobackup(prv);
	DPRINTF("connection made!");


	if (ret) {
		close(fd);
		goto done;
	}

        prv->fd = fd;

done:
	return ret;
}

void tdsyncdr_complete(void *arg, struct tiocb *tiocb, int err)
{
	struct syncdr_request *syncdr = (struct syncdr_request *)arg;
	struct tdsyncdr_state *prv = syncdr->state;

	td_complete_request(syncdr->treq, err);
	prv->syncdr_free_list[prv->syncdr_free_count++] = syncdr;
}

void tdsyncdr_queue_read(td_driver_t *driver, td_request_t treq)
{
	int size;
	uint64_t offset;
	struct syncdr_request *syncdr;
	struct tdsyncdr_state *prv;

	prv    = (struct tdsyncdr_state *)driver->data;
	size   = treq.secs * driver->info.sector_size;
	offset = treq.sec  * (uint64_t)driver->info.sector_size;

	if (prv->syncdr_free_count == 0)
		goto fail;

	syncdr        = prv->syncdr_free_list[--prv->syncdr_free_count];
	syncdr->treq  = treq;
	syncdr->state = prv;

	td_prep_read(&syncdr->tiocb, prv->fd, treq.buf,
		     size, offset, tdsyncdr_complete, syncdr);
	td_queue_tiocb(driver, &syncdr->tiocb);

	return;

fail:
	td_complete_request(treq, -EBUSY);
}

void tdsyncdr_queue_write(td_driver_t *driver, td_request_t treq)
{
	int size;
	uint64_t offset;
	struct syncdr_request *syncdr;
	struct tdsyncdr_state *prv;
	// data for sending request to backup !TW!
	struct req_info *rinfo;
	uint64_t ack;
	int rc;

	/* We need to make the req_info meta data and the data block
	 * adjacement in memory, so allocate a byte array with plenty of
	 * space for both
	 * TODO -- would need to increase this size if req_info struct changes!
	 */
	char req_info_and_data[DR_MAX_WRITE_SIZE+REQ_INFO_SIZE];
	rinfo = (struct req_info *)&req_info_and_data[0];

	prv     = (struct tdsyncdr_state *)driver->data;
	size    = treq.secs * driver->info.sector_size;
	offset  = treq.sec  * (uint64_t)driver->info.sector_size;

	if (prv->syncdr_free_count == 0)
		goto fail;

	syncdr        = prv->syncdr_free_list[--prv->syncdr_free_count];
	syncdr->treq  = treq;
	syncdr->state = prv;

	td_prep_write(&syncdr->tiocb, prv->fd, treq.buf,
		      size, offset, tdsyncdr_complete, syncdr);
	td_queue_tiocb(driver, &syncdr->tiocb);

	/* Send the size and offset, then the actual buffer !TW! */
	rinfo->size = size;
	rinfo->offset = offset;
	rinfo->writeID = prv->pendingWrite++;

	/* Sending meta + block data in one go */
	memcpy(&req_info_and_data[sizeof(struct req_info)], treq.buf, size);

	rc = sendexact(prv->backupSocket,&req_info_and_data[0],size + sizeof(struct req_info));
	if (rc < 0) {
		DPRINTF("ERROR writing meta + buffer to socket");
		return;
	}


	rc = recvexact(prv->backupSocket,(char*) &ack,sizeof(ack));
	if (rc < 0) {
		DPRINTF("ERROR recving ACK from socket");
		return;
	}

	return;

fail:
	td_complete_request(treq, -EBUSY);
}

int tdsyncdr_close(td_driver_t *driver)
{
	struct req_info rinfo; // send zero'd out req_info to indicate close
	int rc;
	struct tdsyncdr_state *prv = (struct tdsyncdr_state *)driver->data;

	bzero(&rinfo, sizeof(struct req_info));

	rc = sendexact(prv->backupSocket, (char*)(&rinfo), sizeof(struct req_info));
	if (rc < 0) {
		DPRINTF("ERROR writing req info to socket");
		return -1;
	}
	close(prv->backupSocket);

	close(prv->fd);

	return 0;
}

int tdsyncdr_get_parent_id(td_driver_t *driver, td_disk_id_t *id)
{
	return TD_NO_PARENT;
}

int tdsyncdr_validate_parent(td_driver_t *driver,
			  td_driver_t *pdriver, td_flag_t flags)
{
	return -EINVAL;
}

struct tap_disk tapdisk_syncdr = {
	.disk_type          = "tapdisk_syncdr",
	.flags              = 0,
	.private_data_size  = sizeof(struct tdsyncdr_state),
	.td_open            = tdsyncdr_open,
	.td_close           = tdsyncdr_close,
	.td_queue_read      = tdsyncdr_queue_read,
	.td_queue_write     = tdsyncdr_queue_write,
	.td_get_parent_id   = tdsyncdr_get_parent_id,
	.td_validate_parent = tdsyncdr_validate_parent,
	.td_debug           = NULL,
};

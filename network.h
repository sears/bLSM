/*
 * network.h
 *
 *  Created on: Feb 2, 2010
 *      Author: sears
 */

#ifndef NETWORK_H_
#define NETWORK_H_

#include <stdio.h>

//server codes
static const uint8_t OP_SUCCESS = 1;
static const uint8_t OP_FAIL = 2;
static const uint8_t OP_SENDING_TUPLE = 3;

//client codes
static const uint8_t OP_FIND = 4;
static const uint8_t OP_INSERT = 5;

static const uint8_t OP_DONE = 6;

static const uint8_t OP_INVALID = 32;


static inline void readfromsocket(int sockd, char *buf, int count)
{

	int n = 0;
	while( n < count )
	{
		n += read( sockd, buf + n, count - n);
	}

}

static inline void writetosocket(int sockd, char *buf, int count)
{
	int n = 0;
	while( n < count )
	{
		n += write( sockd, buf + n, count - n);
	}
}

#endif /* NETWORK_H_ */

#ifndef TLPI_HDR_H
#define TLPI_HDR_H

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <glib.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <arpa/inet.h>

#include "get_num.h"
#include "error_functions.h"


#define min(m,n) ((m) < (n) ? (m) : (n))
#define max(m,n) ((m) > (n) ? (m) : (n))

#define PORT 1975
#define INPUT_BUFSIZE 2048

#define BUF_SIZE 500

#define WAIT_FOR_IMEI 1
#define WAIT_00_01_TOBE_SENT 2
#define WAIT_FOR_DATA_PACKET 3
#define WAIT_NUM_RECIEVED_DATA_TOBE_SENT 4

#define NUM_OF_DATA 9

#endif

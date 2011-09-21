#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <string.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#include <libpq-fe.h>
#include <sys/time.h>
#include <errno.h>
#include <signal.h>

// For some reason epics uses fixed length strings
// Sort of: epics strings are defined as a struct { unsigned length; char *pString}
// but the dbr types (used exclusively by channel access) use epicsOldString defined as
// typedef char epicsOldString[MAX_STRING_SIZE]
//
// I guess we are stuck with fixed length strings here
// that we'll force to have the last byte be null
//
#define MAX_STRING_SIZE 40

typedef struct e_message_header {
  uint16_t cmd;
  uint16_t plsize;
  uint16_t dtype;
  uint16_t dcount;
  uint32_t p1;
  uint32_t p2;
} e_message_header_t;

typedef struct e_extended_message_header {
  uint16_t cmd;
  uint16_t marker1;
  uint16_t dtype;
  uint16_t marker2;
  uint32_t p1;
  uint32_t p2;
  uint32_t plsize;
  uint32_t dcount;
} e_extended_message_header_t;

// response packet
//
typedef struct e_response_struct {
  struct sockaddr_in peer;	// our peer
  int sock;			// our socket
  int bufsize;			// number of bytes used in buf
  char *buf;			// our buffer, free when done
} e_response_t;

// reply queue
//
typedef struct e_reply_queue_struct {
  struct e_reply_queue_struct *next;
  struct sockaddr_in fromaddr;	// our from address to send reply to udp socket
  int fromlen;			// length of from address
  int reply_size;		// number of bytes in reply
  char *reply_packet;		// calloc'ed reply packet
} e_reply_queue_t;

//
// persistent socket information
//
typedef struct e_socks_buffer_struct {
  char *host_name;	// read from host name command
  char *user_name;	// read from user name command
  int sock;		// our socket
  void *buf;		// input buffer
  int active;		// -1 when connection made; otherwise a count of active PV's served, 0 to close tcp connection
  int events_on;	// 1 means send subscription updates, 0 means drop them in the bit bucket
  int bufsize;		// size of the buffer
  char *rbp;		// pointer to the next position in the buffer to read from
  char *wbp;		// pointer to the next position in the buffer to write to
  e_reply_queue_t *reply_q;	// packets ready to send
} e_socks_buffer_t;

typedef struct e_dbr_size_struct {
  char *dbr_name;
  int  dbr_struct_size;
  int  dbr_type_size;
} e_dbr_size_t;



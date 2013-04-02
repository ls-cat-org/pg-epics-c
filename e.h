#define _GNU_SOURCE
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
#include <search.h>

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

//
// dbr size info
//
typedef struct e_dbr_size_struct {
  char *dbr_name;
  int  dbr_struct_size;
  int  dbr_type_size;
} e_dbr_size_t;


typedef struct e_subscription_struct {
  struct e_subscription_struct *next;		// the next in the list
  uint32_t dtype;				// the dbr type requested
  uint32_t dcount;				// the count requested
  uint32_t mask;				// our event mask  1=value changed, 2=log event, 4=alarm event
  uint32_t subid;				// the client's subscription id
} e_subscription_t;

typedef struct e_channel_struct {
  struct e_channel_struct *next;		// the next channel
  char *hostname;				// where the client says its from
  char *username;				// the user the client claims to represent
  uint32_t version;				// the minor protocol version the clients wishes to use
  uint32_t sock;				// the socket this channedl is created on
  uint32_t cid;					// our channel id
  e_subscription_t *subs;			// subscriptions using this channel
} e_channel_t;

typedef struct e_array_struct {
  struct e_array_struct *next;			// the next element
  struct e_kvpair_struct *name;			// the name kvpair
  struct e_kvpair_struct *position;			// the position kvpair
  uint32_t index;				// the index of this item
} e_array_t;

typedef struct e_kvpair_struct {
  struct e_kvpair_struct *next;			// the next in the list
  char *kvname;					// our name
  char *sid_key;				// our alternate key
  char *kvvalue;				// our value
  uint32_t kvseq;				// the sequence number assigned by postgresql
  uint32_t dbr_type;				// our "native" dbr type
  uint32_t eepoch_secs;				// update time in seconds (seconds since 1/1/1990, the epics epoch)
  uint32_t eepoch_nsecs;			// fraction part of the update time in nano seconds
  struct e_kvpair_struct *high_limit;		// kv with our high limit or null if none
  struct e_kvpair_struct *low_limit;		// kv with our low limit or null if none
  struct e_kvpair_struct *high_limit_hit;	// kv that indicates our high limit has been hit
  struct e_kvpair_struct *low_limit_hit;	// kv that indicates our low limit has been hit
  struct e_kvpair_struct *prec;			// the number of decimal places to display
  struct e_kvpair_struct *parent;		// if this is a high/low/prec field then this points to the parent kv
  uint32_t sid;					// Our identifier for this entry
  e_channel_t *chans;				// our subscribers
  e_array_t   *array;				// perhaps we are really an array
  uint32_t array_length;			// length of our perhaps array;
} e_kvpair_t;

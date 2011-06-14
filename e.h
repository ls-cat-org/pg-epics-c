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




// test channel names
//
typedef struct e_chan_struct {
  char *chan_name;	// our name for this channel
  unsigned char type;	// epics type: 0-7 for string, short, float, enum, char, long, double
  unsigned short severity;
  unsigned short status;
  uint32_t timeStampSecs;
  uint32_t timeStampNSecs;
  char *units;
  char **enum_list;
  unsigned short nenums;

  union {
    char *string_value;
    short short_value;
    float  float_value;
    unsigned short enum_value;
    unsigned char char_value;
    long long_value;
    double double_value;
  } current;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } upper_display;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } lower_display;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } upper_alarm;
  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } upper_warning;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } lower_warning;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } lower_alarm;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } upper_control;

  union {
    short short_limit;
    float float_limit;
    unsigned char char_limit;
    long long_limit;
    double double_limit;
  } lower_control;
    
} e_chan_t;

// response packet
//
typedef struct e_response_struct {
  int bufsize;		// number of bytes used in buf
  char *buf;		// our buffer, free when done
} e_response_t;

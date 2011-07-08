#include "e.h"

struct pollfd e_socks[1024];		// array of active sockets
e_socks_buffer_t e_sock_bufs[1024];	// read buffer to support these sockets
int n_e_socks = 0;
int n_e_socks_max = sizeof( e_socks)/sizeof( e_socks[0]);

static PGconn *q = NULL;

char* prepared_statements[] = {
  "prepare beacon_update (inet, int) as select e.beacon_update($1,$2)",
  "prepare channel_search (inet,int,text) as select e.channel_search($1,$2,$3)",
  "prepare create_channel (inet,text,text,int,int,text) as select * from e.create_channel( $1,$2,$3,$4,$5,$6)",
  "prepare get_values (int) as select e.get_values($1)",
  "prepare clear_channel (inet,int,int) as select e.clear_channel($1,$2,$3)",
  "prepare set_str_value (int,text) as select e.set_str_value($1,$2) as rtn"
};

e_dbr_size_t dbr_sizes[] = {
  //
  // Keep in order!!
  //
  { "string", 0, 0},
  { "short", 0, 2},
  { "float", 0, 4},
  { "enum", 0, 2},
  { "char", 0, 1},
  { "long", 0, 4},
  { "double", 0, 8},
  { "sts_string", 4, 0},
  { "sts_short", 4, 2},
  { "sts_float", 4, 4},
  { "sts_enum", 4, 2},
  { "sts_char", 5, 1},
  { "sts_long", 4, 4},
  { "sts_double", 8, 8},
  { "time_string", 12, 0},
  { "time_short", 14, 2},
  { "time_float", 12, 4},
  { "time_enum", 14, 2},
  { "time_char", 15, 1},
  { "time_long", 12, 4},
  { "time_double", 16, 8},
  { "gr_string", 0, 0},
  { "gr_short", 24, 2},
  { "gr_float", 40, 4},
  { "gr_enum", 422, 2},
  { "gr_char", 19, 1},
  { "gr_long", 36, 4},
  { "gr_double", 64, 8},
  { "crtl_string", 0, 0},
  { "crtl_short", 28, 2},
  { "crtl_float", 48, 4},
  { "crtl_enum", 422, 2},
  { "crtl_char", 21, 1},
  { "crtl_long", 44, 4},
  { "crtl_double", 80, 8}
};



PGconn *get_pg_conn() {
  PGresult *pgr;
  int i;

  if( q == NULL) {
    q = PQconnectdb( "dbname=ls user=lsuser host=10.1.0.3");
    if( PQstatus(q) != CONNECTION_OK) {
      fprintf( stderr, "Failed to connect to contrabass (get_pg_conn)\n");
      q = NULL;
    }

    for( i=0; i<sizeof(prepared_statements)/sizeof(prepared_statements[0]); i++) {
      pgr = PQexec( q, prepared_statements[i]);
      if( PQresultStatus( pgr) != PGRES_COMMAND_OK) {
	fprintf( stderr, "Statement preparation failed: %s", PQerrorMessage( q));
      }
      PQclear( pgr);
    }
  }
  return q;
}

// swap double to put in into network byte order
// from http://www.dmh2000.com/cpp/dswap.shtml
//
unsigned long long  swapd(double d) {
  unsigned long long a;
  unsigned char *dst = (unsigned char *)&a;
  unsigned char *src = (unsigned char *)&d;

  dst[0] = src[7];
  dst[1] = src[6];
  dst[2] = src[5];
  dst[3] = src[4];
  dst[4] = src[3];
  dst[5] = src[2];
  dst[6] = src[1];
  dst[7] = src[0];

  return a;
}


// see if this is an extended header
// not used for every command type
//
int get_header_type( unsigned char  *buf) {
  // return 1 if extended header, 0 if normal header
  if( *(buf+2)==0xff && *(buf+3)==0xff && *(buf+6)==0 && *(buf+7)==0) {
    return 1;
  }
  return 0;
}

// return just the command, not the entire header
// Used to avoid supporting the header type for commands
// that never, ever, use an extended header.
//
uint16_t get_command( void *buf) {
  uint16_t tmp;
  memcpy( &tmp, buf, sizeof( uint16_t));
  return ntohs( tmp);
}


void create_message_header( e_message_header_t *mh, uint16_t cmd, uint16_t plsize, uint16_t dtype, uint16_t dcount, uint32_t p1, uint32_t p2) {
  mh->cmd    = htons( cmd);
  mh->plsize = htons( plsize);
  mh->dtype  = htons( dtype);
  mh->dcount = htons( dcount);
  mh->p1     = htonl( p1);
  mh->p2     = htonl( p2);
}

void create_extended_message_header( e_extended_message_header_t *emh, uint16_t cmd, uint32_t plsize, uint16_t dtype, uint32_t dcount, uint32_t p1, uint32_t p2) {
  emh->cmd     = htons( cmd);
  emh->marker1 = 0xffff;
  emh->dtype   = htons( dtype);
  emh->marker2 = 0;
  emh->p1      = htonl( p1);
  emh->p2      = htonl( p2);
  emh->plsize  = htons( plsize);
  emh->dcount  = htons( dcount);
}

// normal header: meaning of fields is command dependent (should be a union, perhaps)
//
void read_message_header( e_socks_buffer_t *inbuf, e_message_header_t *h) {
  memcpy( h, inbuf->rbp, sizeof( struct e_message_header));
  inbuf->rbp += sizeof( struct e_message_header);

  h->cmd    = ntohs( h->cmd);
  h->plsize = ntohs( h->plsize);
  h->dtype  = ntohs( h->dtype);
  h->dcount = ntohs( h->dcount);
  h->p1     = ntohl( h->p1);
  h->p2     = ntohl( h->p2);
}

// extended header: meaning of fields is command dependent (haha)
// converts a regular header into an extended header so other functions do
// not need to support both
//
void read_extended_message_header( e_socks_buffer_t *inbuf, e_extended_message_header_t *h) {
  struct e_message_header mh;

  if( get_header_type( inbuf->rbp)) {
    h->cmd    = ntohs( h->cmd);
    h->dtype  = ntohs( h->dtype);
    h->p1     = ntohl( h->p1);
    h->p2     = ntohl( h->p2);
    h->plsize = ntohl( h->plsize);
    h->dcount = ntohl( h->dcount);

    inbuf->rbp += sizeof( e_extended_message_header_t);
    return;
  }

  //
  // normal header: put it into an extended header
  //
  read_message_header( inbuf, &mh);
  h->cmd    = mh.cmd;
  h->dtype  = mh.dtype;
  h->p1     = mh.p1;
  h->p2     = mh.p2;
  h->plsize = mh.plsize;
  h->dcount = mh.dcount;
}




// cmd 0
// tcp and udp
void cmd_ca_proto_version( e_socks_buffer_t *inbuf, e_response_t *r) {
  // 
  // Docs specify that this command does not request a response but our
  // implementation will perceive that it does (?)
  //
  // for protocol 4.11 (ours) "Server will send response immediately after establishing a virtual circuit"
  // but the example given does not include such a response.
  //
  // Docs also specify that field p1 "Must be 0." but it looks to contain a counter.
  //
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
}



// cmd 1
// tcp
void cmd_ca_proto_event_add( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  printf( "Event add\n");

  inbuf->rbp += emh.plsize;
}

// cmd 2
// tcp
void cmd_ca_proto_event_cancel( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;
  read_extended_message_header( inbuf, &emh);

  printf( "Event cancel\n");

  inbuf->rbp += emh.plsize;
}

// cmd 3
// tcp
void cmd_ca_proto_read( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  printf( "Proto Read\n");

  inbuf->rbp += emh.plsize;
}

// cmd 4
// tcp
void cmd_ca_proto_write( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;
  char *payload;
  void *params[3];
  int param_lengths[3];
  int param_formats[3];
  uint32_t struct_size;
  uint32_t data_size, s_size;
  char *sp;
  PGresult *pgr;
  uint32_t dbr_type;
  uint32_t ioid, sid, nsid;

  read_extended_message_header( inbuf, &emh);
  sid = emh.p1;
  nsid = htonl(sid);
  ioid = emh.p2;
  emh.dcount = 1;	// hold the arrays

  //
  // Discover our data size (shouldn't we just create an array at initiallizaion or compile time?...)
  //
  dbr_type = htonl(emh.dtype);
  struct_size = dbr_sizes[emh.dtype].dbr_struct_size;
  data_size   = dbr_sizes[emh.dtype].dbr_type_size;

  //
  // TO DO:
  // add proper array support by implementing the postgresql/libpq array structures
  //
  payload = inbuf->rbp;
  switch( emh.dtype) {
  case 0:
    sp = payload;
    s_size = strlen( sp)+1;
    printf( "Proto Write: %s\n", sp);
    if( payload + s_size > inbuf->wbp) {
      fprintf( stderr, "Bad string detected (cmd_ca_proto_write_notify)\n");
      inbuf->rbp = inbuf->wbp;
      return;
    }
    payload += s_size + 1;
    params[0] = &nsid;	param_lengths[0] = sizeof(nsid);	param_formats[0] = 1;
    params[1] = sp;		param_lengths[1] = 0;			param_formats[1] = 0;
    pgr = PQexecPrepared( q, "set_str_value", 2, (const char **)params, param_lengths, param_formats, 0);
    if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
      fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
      PQclear( pgr);
      inbuf->rbp += emh.plsize;
      return;
    }
    PQclear( pgr);
  }
  inbuf->rbp += emh.plsize;
}

// cmd 5
// tcp
void cmd_ca_proto_snapshot( e_socks_buffer_t *inbuf, e_response_t *r) {
  // obsolete
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
}

// cmd 6
// tcp and udp
void cmd_ca_proto_search( e_socks_buffer_t *inbuf, e_response_t *r) {
  int reply;
  int version, versionn;
  int cid;
  char *pl;
  int foundIt;
  char *brvp;  // pointer to the boolean returned value

  e_extended_message_header_t emh;
  char* params[3];
  int   paramLengths[3];
  int   paramFormats[3];
  PGresult *pgr;

  read_extended_message_header( inbuf, &emh);
  pl = inbuf->rbp;
  pl[emh.plsize-1] = 0;
  inbuf->rbp += emh.plsize;

  reply   = emh.dtype;
  version = emh.dcount;
  versionn = htonl( version);
  cid     = emh.p1;

  fprintf( stderr, "Search: plsize = %d, version = %d, reply = %d, cid = %d, PV = '%s'\n", emh.plsize, version, reply, cid, pl);

  params[0] = inet_ntoa( r->peer->sin_addr);	paramLengths[0] = 0;                   paramFormats[0] = 0;
  params[1] = (char *)&versionn;                paramLengths[1] = sizeof( versionn);   paramFormats[1] = 1;
  params[2] = pl;                               paramLengths[2] = 0;                   paramFormats[2] = 0;

  foundIt = 0;
  pgr = PQexecPrepared( q, "channel_search", 3, (const char **)params, paramLengths, paramFormats, 1);
  if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
    fprintf( stderr, "Statement execution failed (cmd_ca_proto_search): %s", PQerrorMessage( q));
  }
  if( PQgetisnull( pgr, 0, 0) == 0) {
    // only look at a non-null reply
    if( PQgetlength( pgr, 0, 0) != 1) {
      fprintf( stderr, "Warning: channel_search returned a value of length %d instead of 1 as expected (cmd_ca_proto_search)\n", PQgetlength( pgr, 0, 0));
    } else {
      brvp = (char *)PQgetvalue( pgr, 0, 0);
      if( *brvp != 0) {
	fprintf( stderr, "Found channel %s\n", pl);
	foundIt = 1;
      }
    }
  }
  PQclear( pgr);

  if( foundIt) {
    uint16_t server_protocol_version = 11, *spvp;

    r->bufsize = sizeof( e_message_header_t) + 8;
    r->buf     = calloc( r->bufsize, 1);
    if( r->buf == NULL) {
      fprintf( stderr, "Out of memory (cmd_ca_proto_search)\n");
      return;
    }
    create_message_header( (e_message_header_t *)r->buf, 6, 8, 5064, 0, 0xffffffff, cid);
    spvp = (uint16_t *)(r->buf + sizeof( e_message_header_t));
    *spvp = htons(server_protocol_version);
  }


  if( !foundIt && reply == 10) {
    // Docs specify that reply==10 only on a TCP request and that the reply
    // should come back over UDP.  We'll just send the reply back over the same socket
    // it came in on and assume that either the documentation or the protocol are wacky
    //
    r->bufsize = sizeof( e_message_header_t);
    r->buf     = calloc( r->bufsize, 1);
    if( r->buf == NULL) {
      fprintf( stderr, "Out of memory (cmd_ca_proto_search)\n");
      return;
    }
    create_message_header( (e_message_header_t *)r->buf, 14, 0, 10, version, cid, cid);
  }
}


// cmd 7
// tcp
void cmd_ca_proto_build( e_socks_buffer_t *inbuf, e_response_t *r) {
  // obsolete
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
}

// cmd 8
// tcp
void cmd_ca_proto_events_off( e_socks_buffer_t *inbuf, e_response_t *r) {
  //
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Events off\n");
}

// cmd 9
// tcp
void cmd_ca_proto_events_on( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Events on\n");
}

// cmd 10
// tcp
void cmd_ca_proto_read_sync( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Read Sync\n");
}

// cmd 11
// tcp
void cmd_ca_proto_error( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Proto Error\n");
}

// cmd 12
// tcp
void cmd_ca_proto_clear_channel( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t sid, cid, sidn, cidn;
  e_extended_message_header_t emh;
  char* params[3];
  int param_lengths[3];
  int param_formats[3];
  PGresult *pgr;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  sid = emh.p1;
  sidn = htonl( sid);
  cid = emh.p2;
  cidn = htonl( cid);
  printf( "Clear channel sid: %d, cid: %d\n", sid, cid);

  params[0] = inet_ntoa( r->peer->sin_addr); param_lengths[0] = 0;                param_formats[0] = 0;
  params[1] = (char *)&sidn;                 param_lengths[1] = sizeof(sidn);     param_formats[1] = 1;
  params[2] = (char *)&cidn;                 param_lengths[2] = sizeof(cidn);     param_formats[2] = 1;
  
  pgr = PQexecPrepared( q, "clear_channel", 3, (const char **)params, param_lengths, param_formats, 0);
  if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
    fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
  }
  PQclear( pgr);

  r->bufsize = sizeof( e_message_header_t);
  r->buf     = calloc( r->bufsize, 1);
  if( r->buf == NULL) {
    fprintf( stderr, "Out of memory (cmd_ca_proto_clear_channel)\n");
  } else {
    create_message_header( (e_message_header_t *)r->buf, 12, 0, 0, 0, 1, cid);
  }
  inbuf->active--;
}

// cmd 13
// udp
void cmd_ca_proto_rsrv_is_up( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t beaconid, nbeaconid;

  struct in_addr addr;
  e_extended_message_header_t emh;
  void *params[2];
  int param_lengths[2];
  int param_formats[2];
  PGresult *pgr;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  beaconid = emh.p1;

  if( emh.p2 == 0) {
    addr = r->peer->sin_addr;
  } else {
    addr.s_addr     = htonl(emh.p2);
  }

  nbeaconid = htonl( beaconid);

  params[0] = inet_ntoa( addr); param_lengths[0] = 0;                  param_formats[0] = 0;
  params[1] = &nbeaconid;       param_lengths[1] = sizeof( nbeaconid); param_formats[1] = 1;
  pgr = PQexecPrepared( q, "beacon_update", 2, (const char **)params, param_lengths, param_formats, 0);
  if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
    fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
  }
  PQclear( pgr);

  // printf( "Beacon from %s with id %d\n", inet_ntoa( addr), beaconid);
}

// cmd 14
// tcp and udp
void cmd_ca_proto_not_found( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Not Found\n");
}

// cmd 15
// tcp
void cmd_ca_proto_read_notify( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;
  uint32_t sid, nsid;
  uint32_t ioid;
  void *payload;
  uint32_t dbr_type;
  void *params[3];
  int   param_lengths[3];
  int   param_formats[3];
  int   struct_size;
  int   data_size;
  int   payload_size;
  char *svalue;
  PGresult *pgr;

  read_extended_message_header( inbuf, &emh);
  sid  = emh.p1;
  ioid = emh.p2;
  emh.dcount = 1;	// don't handle arrays just yet
  dbr_type = htonl(emh.dtype);

  struct_size = dbr_sizes[emh.dtype].dbr_struct_size;
  data_size   = dbr_sizes[emh.dtype].dbr_type_size;

  fprintf( stderr, "struct_size: %d, data_size: %d, sid: %u, ioid: %u\n", struct_size, data_size, sid, ioid);

  nsid = htonl(sid);
  params[0] = &nsid;		param_lengths[0] = sizeof( nsid);	param_formats[0] = 1;
  //
  pgr = PQexecPrepared( q, "get_values", 1, (const char **)params, param_lengths, param_formats, 0);
  if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
    fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
    PQclear( pgr);
    return;
  }
  svalue = PQgetvalue( pgr, 0, 0);


  if( emh.dtype % 7 == 0) {
    // payload is a null terminated string
    payload_size = struct_size + strlen(svalue);
  } else {
    // payload is a fixed size entity
    payload_size = struct_size + data_size * emh.dcount;
  }
  // payload size must be evenly divisible by 8
  payload_size += (8 - (payload_size % 8));

  fprintf( stderr, "Got value = '%s', struct size = %d, payload size = %d, data size = %d, data count = %d, dbr type = %d (e_cmd_read_notify)\n",
	   svalue, struct_size, payload_size, data_size, emh.dcount, emh.dtype);


  r->bufsize = payload_size + sizeof( e_message_header_t);
  r->buf = calloc( r->bufsize, 1);
  create_message_header( (e_message_header_t *)r->buf, 15, payload_size, emh.dtype, emh.dcount, 1, ioid);
  payload = r->buf + sizeof( e_message_header_t);
  //
  // this is where we'd fill in the structure stuff.  leave it zero for now.
  //
  //statusp   = payload;
  //  *statusp  = htons( 1);
  //  severityp = payload + 2;

  payload += struct_size;
  
  switch( emh.dtype % 7) {
    char char_value;
    int16_t short_value;
    int32_t int_value, int_value2;
    float   float_value;
    double  double_value;
    long long long_long_value;

  case 0:	// string
    strcpy( payload, svalue);
    break;

  case 1:	// short
    short_value = htons( atoi( svalue));
    memcpy( payload, &short_value, sizeof( short_value));
    break;

  case 2:	// float
    float_value = atof( svalue);
    memcpy( &int_value, &float_value, sizeof( int_value));
    int_value2 = htonl( int_value);
    memcpy( payload, &int_value2, sizeof( int_value2));
    break;

  case 3:	// enum
    short_value = htons( atoi( svalue));
    memcpy( payload, &short_value, sizeof( short_value));
    break;

  case 4:	// char
    char_value = atoi( svalue);
    memcpy( payload, &short_value, sizeof( char_value));
    break;

  case 5:	// int
    int_value = htonl( atoi( svalue));
    memcpy( payload, &int_value, sizeof( int_value));
    break;

  case 6:	// double
    double_value = atof( svalue);
    long_long_value = swapd( double_value);
    memcpy( payload, &long_long_value, sizeof( long_long_value));
    break;
  }

  PQclear( pgr);
  
  inbuf->rbp += emh.plsize;
  printf( "Read Notify for sid=%d\n", sid);
}

// cmd 16
// tcp
// Obsolete
void cmd_ca_proto_read_build( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Read Build\n");
}

// cmd 17
// udp
// todo
void cmd_ca_repeater_confirm( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Repeater Confirm\n");
}

// cmd 18
// tcp
void cmd_ca_proto_create_chan( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t cid, cidn;
  uint32_t version, versionn;
  uint32_t sid, *sidp;
  
  char* params[6];
  int   paramLengths[6];
  int   paramFormats[6];
  PGresult *pgr;
  char *payload;
  e_extended_message_header_t emh;
  e_message_header_t *h1, *h2, *h3;

  read_extended_message_header( inbuf, &emh);
  payload = inbuf->rbp;		// pointer to our string
  payload[emh.plsize-1] = 0;	// ensure it is null terminated
  inbuf->rbp += emh.plsize;
  cid = emh.p1;
  version = emh.p2;

  printf( "Create Chan with name '%s'\n", payload);
  if( inbuf->host_name == NULL)
    inbuf->host_name = strdup("");
  if( inbuf->user_name == NULL)
    inbuf->user_name = strdup("");
  cidn = htonl( cid);
  versionn = htonl( version);

  params[0] = inet_ntoa( r->peer->sin_addr); paramLengths[0] = 0;                paramFormats[0] = 0;
  params[1] = inbuf->host_name;	             paramLengths[1] = 0;                paramFormats[1] = 0;
  params[2] = inbuf->user_name;              paramLengths[2] = 0;                paramFormats[2] = 0;
  params[3] = (char *)&cidn;                 paramLengths[3] = sizeof(cidn);     paramFormats[3] = 1;
  params[4] = (char *)&versionn;             paramLengths[4] = sizeof(versionn); paramFormats[4] = 1;
  params[5] = payload;	                     paramLengths[5] = 0;                paramFormats[5] = 0;


  pgr = PQexecPrepared( q, "create_channel", 6, (const char **)params, paramLengths, paramFormats, 1);
  if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
    fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
  }
  if( PQntuples( pgr) > 0 && PQgetisnull( pgr, 0, 0) != 1) {
    //
    // Success
    //
    sidp = (uint32_t *)PQgetvalue( pgr, 0, PQfnumber( pgr, "sid"));
    sid = ntohl( *sidp);

    r->bufsize = 3*sizeof( e_message_header_t);
    r->buf     = calloc( r->bufsize, 1);
    if( r->buf == NULL) {
      fprintf( stderr, "Out of memory (cmd_ca_proto_create_chan)\n");
    } else {
      h1 = (e_message_header_t *)r->buf;
      h2 = h1 + 1;
      h3 = h2 + 1;
      create_message_header( h1,  0, 0, 0, 11,   0,   0);	// protocol response
      create_message_header( h2, 22, 0, 0,  0, cid,   3);	// grant read (1) and write (2) access
      create_message_header( h3, 18, 0, 0,  1, cid, sid);	// channel create response

      if( inbuf->active == -1) {
	inbuf->active = 1;
      } else {
	inbuf->active++;
      }
    }
  } else {
    //
    // Failed to create channel
    //
    r->bufsize = sizeof( e_message_header_t);
    r->buf = calloc( r->bufsize, 1);
    create_message_header( (e_message_header_t *)r->buf, 26, 0, 0, 0, cid, 0);

    if( inbuf->active == -1) {
      //
      // First attempt after the connection
      // just let it die
      //
      inbuf->active = 0;
    }
  }

  PQclear( pgr);
}

// cmd 19
// tcp
void cmd_ca_proto_write_notify( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;
  void *params[3];
  int param_lengths[3];
  int param_formats[3];
  uint32_t struct_size;
  uint32_t data_size;
  PGresult *pgr;
  uint32_t dbr_type;
  uint32_t ioid, sid, nsid;
  int rtn;
  char *sp;
  int s_size;
  int rtn_value;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  
  sid = emh.p1;
  ioid = emh.p2;
  emh.dcount = 1;	// hold the arrays
  //
  // Discover our data size (shouldn't we just create an array at initiallizaion or compile time?...)
  //
  dbr_type = htonl(emh.dtype);
  struct_size = dbr_sizes[emh.dtype].dbr_struct_size;
  data_size   = dbr_sizes[emh.dtype].dbr_type_size;

  r->bufsize = sizeof( e_message_header_t);
  r->buf = calloc( r->bufsize, 1);
  if( r->buf == NULL) {
    fprintf( stderr, "Out of memory (cmd_ca_proto_write_notify)\n");
    return;
  }

  rtn = 160;	// default ca put fail
  //
  // TO DO:
  // add proper array support by implementing the postgresql/libpq array structures
  //
  switch( emh.dtype) {
  case 0:
    sp = inbuf->rbp;
    s_size = strlen( sp);
    if( inbuf->rbp + s_size > inbuf->wbp) {
      fprintf( stderr, "Bad string detected (cmd_ca_proto_write_notify)\n");
      inbuf->rbp = inbuf->wbp;
      return;
    }
    inbuf->rbp += s_size;
    params[0] = &nsid;	param_lengths[0] = sizeof(nsid);	param_formats[0] = 1;
    params[1] = sp;		param_lengths[1] = 0;			param_formats[1] = 0;
    pgr = PQexecPrepared( q, "set_str_value", 2, (const char **)params, param_lengths, param_formats, 1);
    if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
      PQclear( pgr);
      return;
      fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
    }
    rtn_value = ntohl( *(uint32_t *)PQgetvalue( pgr, 0, PQfnumber( pgr, "rtn")));
    PQclear( pgr);
  }
  nsid = htonl(sid);
  create_message_header( (e_message_header_t *)r, 19, 0, emh.dtype, emh.dcount, rtn_value, ioid);
}

// cmd 20
// tcp
void cmd_ca_proto_client_name( e_socks_buffer_t *inbuf, e_response_t *r) {
  char *clientName;
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  clientName = inbuf->rbp;
  clientName[emh.plsize - 1] = 0;
  inbuf->rbp += emh.plsize;

  if( inbuf->user_name != NULL) {
    free( inbuf->user_name);
  }
  inbuf->user_name = strdup( clientName);

  printf( "Client Name '%s'\n", clientName);
  //
  // no response
  //
}

// cmd 21
// tcp
void cmd_ca_proto_host_name( e_socks_buffer_t *inbuf, e_response_t *r) {
  //
  char *hostName;
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  hostName = inbuf->rbp;
  hostName[emh.plsize - 1] = 0;
  inbuf->rbp += emh.plsize;

  if( inbuf->host_name != NULL) {
    free( inbuf->host_name);
  }
  inbuf->host_name = strdup( hostName);

  printf( "Host Name '%s'\n", hostName);
  //
  // no response
  //
}

// cmd 22
// tcp
void cmd_ca_proto_access_rights( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Access Rights\n");
}

// cmd 23
// tcp and udp
void cmd_ca_proto_echo( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Echo\n");

  r->bufsize = sizeof( e_message_header_t);
  r->buf = calloc( r->bufsize, 1);
  if( r->buf == NULL) {
    fprintf( stderr, "Out of memory (cmd_ca_proto_echo)\n");
    return;
  }
  create_message_header( (e_message_header_t *)r->buf, 23, 0, 0, 0, 0, 0);
}

// cmd 24
// udp
void cmd_ca_repeater_register( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Repeater Register\n");
}

// cmd 25
// tcp
// Obsolete
void cmd_ca_proto_signal( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Signal\n");
}

// cmd 26
// tcp
void cmd_ca_proto_create_ch_fail( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Create ch fail\n");
}

// cmd 27
// tcp
void cmd_ca_proto_server_disconn( e_socks_buffer_t *inbuf, e_response_t *r) {
  e_extended_message_header_t emh;

  read_extended_message_header( inbuf, &emh);
  inbuf->rbp += emh.plsize;
  printf( "Server Disconnect\n");
}



void (*cmds[])(e_socks_buffer_t *, e_response_t *) = {
  cmd_ca_proto_version,		//  0
  cmd_ca_proto_event_add,	//  1
  cmd_ca_proto_event_cancel,	//  2
  cmd_ca_proto_read,		//  3
  cmd_ca_proto_write,		//  4
  cmd_ca_proto_snapshot,	//  5
  cmd_ca_proto_search,		//  6
  cmd_ca_proto_build,		//  7
  cmd_ca_proto_events_off,	//  8
  cmd_ca_proto_events_on,	//  9
  cmd_ca_proto_read_sync,	// 10
  cmd_ca_proto_error,		// 11
  cmd_ca_proto_clear_channel,	// 12
  cmd_ca_proto_rsrv_is_up,	// 13
  cmd_ca_proto_not_found,	// 14
  cmd_ca_proto_read_notify,	// 15
  cmd_ca_proto_read_build,	// 16
  cmd_ca_repeater_confirm,	// 17
  cmd_ca_proto_create_chan,	// 18
  cmd_ca_proto_write_notify,	// 19
  cmd_ca_proto_client_name,	// 20
  cmd_ca_proto_host_name,	// 21
  cmd_ca_proto_access_rights,	// 22
  cmd_ca_proto_echo,		// 23
  cmd_ca_repeater_register,	// 24
  cmd_ca_proto_signal,		// 25
  cmd_ca_proto_create_ch_fail,	// 26
  cmd_ca_proto_server_disconn	// 27
};


void fixup_bps( e_socks_buffer_t *b) {
  int nbytes;

  if( b==NULL || b->buf==NULL) {
    fprintf( stderr, "Bad buffer pointer (why?) (fixup_bps)\n");
    return;
  }
  if( b->wbp < b->rbp) {
    fprintf( stderr, "Read and write buffer out of sync? (fixup_bps)\n");
    return;
  }
  nbytes = b->wbp - b->rbp;
  memmove( b->buf, b->rbp, nbytes);
  b->rbp = b->buf;
  b->wbp = b->buf + nbytes;
}

void ca_service( struct pollfd *pfd, e_socks_buffer_t *inbuf) {
  static struct sockaddr_in fromaddr;	// client's address
  static int fromlen;			// used and ignored to store length of client address
  static e_response_t ert[1024];	// our responses
  e_extended_message_header_t bad_cmd_header;	// used to skip commands we do not know how to handle
  void *old_rbp;			// used to be sure we are still reading from the buffer
  int nert;				// number of responses
  int i;				// loop over responses
  int rsize;				// size of reply packet
  void *reply;				// our reply
  void *rp;				// pointer to next location to write into reply
  int cmd;				// our current command
  int nread;				// number of bytes read
  
  fixup_bps( inbuf);

  fromlen = sizeof( fromaddr);
  nread = recvfrom( pfd->fd, inbuf->wbp, inbuf->bufsize - (inbuf->wbp - inbuf->rbp), 0, (struct sockaddr *) &fromaddr, &fromlen);
  if( nread == -1 || nread == 0) {
    // we should stick some error handling code here
    // for now we assume the UDP listening socket is not going to close on its own
    //
    return;
  }
  inbuf->wbp += nread;

  printf( "From %s port %d read %d bytes\n", inet_ntoa( fromaddr.sin_addr), ntohs(fromaddr.sin_port), nread);

  nert = 0;
  while( inbuf->rbp < inbuf->wbp) {

    old_rbp = inbuf->rbp;
    cmd = get_command( inbuf->rbp);
    if( cmd <0 || cmd > 27) {
      read_extended_message_header( inbuf, &bad_cmd_header);
      inbuf->rbp += bad_cmd_header.plsize;
      fprintf( stderr, "unsupported command %d with payload size %d\n", cmd, bad_cmd_header.plsize);
      if( inbuf->rbp > inbuf->wbp) {
	fprintf( stderr, "request to read more bytes than we have: likely we've screwed up the buffer, reseting\n");
	inbuf->rbp = inbuf->buf;
	inbuf->wbp = inbuf->buf;
	return;
      }
    } else {
      if( 1 || (cmd != 6 && cmd != 0 && cmd != 13)) {
	// don't print out common commands
	printf( "Cmd %d: ", cmd);
	fflush( stdout);
      }
      ert[nert].peer    = &fromaddr;
      ert[nert].bufsize = 0;
      ert[nert].buf     = NULL;
      cmds[cmd](inbuf, (ert+nert));

      if( ert[nert].bufsize >= 0) {
	// save responses for the end
	nert++;
	if( nert > (sizeof( ert) / sizeof( ert[0]))) {
	  //
	  // Really, this should not happen in real life.
	  //
	  break;
	}
      }
    }
    if( inbuf->rbp == old_rbp) {
      // nothing left we can read
      break;
    }
    
    // make up reply packet
    // First get its size
    rsize = 0;
    for( i=0; i<nert; i++) {
      rsize += ert[i].bufsize;
    }

    if( rsize>0) {
      reply = calloc( rsize, 1);
      if( reply == NULL) {
	fprintf( stderr, "Out of memory for reply (ca_service)\n");
	exit( -1);
      }
      rp = reply;
      for( i=0; i<nert; i++) {
	if( ert[i].bufsize && ert[i].buf != NULL) {
	  memcpy( rp, ert[i].buf, ert[i].bufsize);
	  rp += ert[i].bufsize;
	  free( ert[i].buf);
	  ert[i].buf = NULL;
	  ert[i].bufsize = 0;
	}
      }
      
      // could save a malloc/free by using the MSG_MORE flag in sendto...
      //
      sendto( pfd->fd, reply, rsize, 0, (const struct sockaddr *)&fromaddr, fromlen);
      
      free( reply);
      reply = NULL;
      rsize = 0;
    }

  }
  printf( "\n");
}

void e_socks_buf_init( int sock) {
  int i = n_e_socks;

  e_socks[i].fd            = sock;
  e_socks[i].events        = POLLIN;
  e_sock_bufs[i].host_name = NULL;
  e_sock_bufs[i].user_name = NULL;
  e_sock_bufs[i].active    = -1;
  e_sock_bufs[i].bufsize   = 4096;
  e_sock_bufs[i].buf       = calloc( e_sock_bufs[i].bufsize, 1);
  if( e_sock_bufs[i].buf == NULL) {
    fprintf( stderr, "out of memory for sock %d (e_socks_buf_init)\n", sock);
  }
  e_sock_bufs[i].rbp       = e_sock_bufs[i].buf;
  e_sock_bufs[i].wbp       = e_sock_bufs[i].buf;

  n_e_socks++;
}

void vclistener_service( struct pollfd *pfd, e_socks_buffer_t *inbuf) {
  static struct sockaddr_in fromaddr;	// client's address
  static int fromlen;			// used and ignored to store length of client address
  int newsock;
  
  fromlen = sizeof( fromaddr);
  newsock = accept( pfd->fd, (struct sockaddr *)&fromaddr, &fromlen);

  fprintf( stderr, "accepted socket %d from %s (vclistener_service)\n", newsock, inet_ntoa( fromaddr.sin_addr));

  if( newsock < 0) {
    return;
  }
  if( n_e_socks < n_e_socks_max) {
    e_socks_buf_init( newsock);
  }
}


int main( int argc, char **argv) {
  static int sock, beacons;		// our main socket
  static int vclistener;		// our tcp listener
  static struct sockaddr_in addr;	// our address

  int err;				// error return from bind
  int i;				// loop for poll response and sockets
  int nfds;				// number of active file descriptors from poll
  int flags;				// used to set non-blocking io for vclistener

  //
  // pgres
  //
  get_pg_conn();

  //
  // UDP comms
  //
  sock = socket( PF_INET, SOCK_DGRAM, 0);
  if( sock == -1) {
    fprintf( stderr, "Could not create udp socket\n");
    exit( -1);
  }
  
  addr.sin_family = AF_INET;
  addr.sin_port   = htons(5064);
  addr.sin_addr.s_addr = INADDR_ANY;

  err = bind( sock, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
  if( err == -1) {
    fprintf( stderr, "Cound not bind socket\n");
    exit( -1);
  }

  e_socks_buf_init( sock);

  beacons = socket( PF_INET, SOCK_DGRAM, 0);
  if( sock == -1) {
    fprintf( stderr, "Could not create udp beacon socket\n");
    exit( -1);
  }
  
  addr.sin_family = AF_INET;
  addr.sin_port   = htons(5065);
  addr.sin_addr.s_addr = INADDR_ANY;

  err = bind( beacons, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
  if( err == -1) {
    fprintf( stderr, "Cound not bind beacon socket\n");
    exit( -1);
  }

  e_socks_buf_init( beacons);

  //
  // TCP Virtual Circuits
  //
  vclistener = socket( PF_INET, SOCK_STREAM, 0);
  if( vclistener == -1) {
    fprintf( stderr, "Could not create virtual circuit listener socket\n");
    exit( -1);
  }
  
  //
  // non-blocking io for vclistener so accept does not
  // hang if the client dies
  //
  flags = fcntl( vclistener, F_GETFL, 0);
  fcntl( vclistener, F_SETFL, flags | O_NONBLOCK);

  addr.sin_family = AF_INET;
  addr.sin_port   = htons(5064);
  addr.sin_addr.s_addr = INADDR_ANY;

  err = bind( vclistener, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
  if( err == -1) {
    fprintf( stderr, "Cound not bind virtual circuit listener socket\n");
    exit( -1);
  }

  err = listen( vclistener, 4);
  if( err == -1) {
    fprintf( stderr, "Could not listen with the virtual circuit listener socket\n");
    exit( -1);
  }
  
  e_socks_buf_init( vclistener);


  while( 1) {
    for( i=0; i<n_e_socks; i++) {
      //
      // root out all the inactive sockets
      //
      if( e_sock_bufs[i].active == 0) {
	
	close( e_socks[i].fd);
	n_e_socks--;
	if( i == n_e_socks) {
	  // no need to do any more work to remove this socket
	  break;
	}
	while( e_sock_bufs[n_e_socks].active == 0 && n_e_socks > i) {
	  // find the last active socket
	  n_e_socks--;
	}
	if( n_e_socks > i) {
	  // move it into the current position
	  e_sock_bufs[i] = e_sock_bufs[n_e_socks];
	  e_socks[i]     = e_socks[n_e_socks];
	}
      }
    }
    
    nfds = poll( e_socks, n_e_socks, -1);
    for( i=0; nfds>0 && i<n_e_socks_max; i++) {
      if( e_socks[i].revents) {
	nfds--;
	
	if( e_socks[i].fd == vclistener) {
	  vclistener_service( e_socks+i, e_sock_bufs+i);
	} else {
	  ca_service( e_socks+i, e_sock_bufs+i);
	}
      }
    }
  }
    return 0;
  }

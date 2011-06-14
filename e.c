#include "e.h"

e_chan_t our_chans[1024];
int n_chans = 0;
int max_n_chans = sizeof( our_chans)/sizeof(our_chans[0]);

struct pollfd e_socks[1024];
int n_e_socks = 0;
int n_e_socks_max = sizeof( e_socks)/sizeof( e_socks[0]);

e_chan_t *get_next_new_chan( char *chan_name, char *chan_units) {
  e_chan_t *rtn;

  if( n_chans == max_n_chans) {
    return NULL;
  }
  rtn = our_chans + n_chans;
  n_chans++;

  chan->chan_name = strdup( chan_name);
  chan->severity  = 0;	// 0=NO_ALARM, 1=MINOR, 2=MAJOR, 3=INVALID
  chan->status    = 0;  // 0=NO_ALARM, menuAlarmStat.h
  chan->timeStampSecs   = time();	// should use gettimeofday
  chan->timeStampNSecs  = 0;
  chan->units     = strdup( chan_units);
  chan->enum_list = NULL;
  chan->nenums    = 0;
  return rtn;
}

void init_chan_string( char *chan_name, char *value) {
  e_chan_t *chan;

  chan = get_next_new_chan( chan_name, "");
  if( chan == NULL) {
    fprintf( strerr, "Out of Channel Space for channel %s\n", chan_name);
    return;
  }
  chan->current.string_value = strdup( value);
}

void init_chan_short( char *chan_name, short value, char *units) {
  e_chan_t *chan;

  chan = get_next_new_chan( chan_name, units);
  if( chan == NULL) {
    fprintf( strerr, "Out of Channel Space for channel %s\n", chan_name);
    return;
  }
  chan->current.short_value = value
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
  mh->p2     = mtonl( p2);
}

void create_extended_message_header( e_message_header_t *emh, uint16_t cmd, uint32_t plsize, uint16_t dtype, uint32_t dcount, uint32_t p1, uint32_t p2) {
  emh->cmd     = htons( cmd);
  emh->marker1 = 0xffff;
  emh->dtype   = htons( dtype);
  emh->marker2 = 0;
  emh->p1      = htonl( p1);
  emh->p2      = mtonl( p2);
  emh->plsize  = htons( plsize);
  emh->dcount  = htons( dcount);
}

// normal header: meaning of fields is command dependent (should be a union, perhaps)
//
int read_message_header( void *buf, e_message_header_t *h) {
  memcpy( h, buf, sizeof( struct e_message_header));
  h->cmd    = ntohs( h->cmd);
  h->plsize = ntohs( h->plsize);
  h->dtype  = ntohs( h->dtype);
  h->dcount = ntohs( h->dcount);
  h->p1     = ntohl( h->p1);
  h->p2     = ntohl( h->p2);
  return sizeof( struct e_message_header);
}

// extended header: meaning of fields is command dependent (haha)
// converts a regular header into an extended header so other functions do
// not need to support both
//
int read_extended_message_header( void *buf, e_extended_message_header_t *h) {
  struct e_message_header mh;
  int rtn;

  if( get_header_type( buf)) {
    memcpy( h, buf, sizeof( e_extended_message_header_t));
    h->cmd    = ntohs( h->cmd);
    h->dtype  = ntohs( h->dtype);
    h->p1     = ntohl( h->p1);
    h->p2     = ntohl( h->p2);
    h->plsize = ntohl( h->plsize);
    h->dcount = ntohl( h->dcount);
    return sizeof( e_extended_message_header_t);
  }
  //
  // normal header: put it into an extended header
  //
  rtn = read_message_header( buf, &mh);
  h->cmd    = mh.cmd;
  h->dtype  = mh.dtype;
  h->p1     = mh.p1;
  h->p2     = mh.p2;
  h->plsize = mh.plsize;
  h->dcount = mh.dcount;
  return rtn;
}




// cmd 0
// tcp and udp
int cmd_ca_proto_version( void *buf, e_response_t *r) {
  int rtn;
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
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  //printf( "Protocol version %d\n", emh.dcount);
  return rtn;
}



// cmd 1
// tcp
int cmd_ca_proto_event_add( void *buf, e_response_t *r) {
  int rtn;

  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;

  printf( "Event add\n");

  return rtn;
}

// cmd 2
// tcp
int cmd_ca_proto_event_cancel( void *buf, e_response_t *r) {
  int rtn;

  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;

  printf( "Event cancel\n");

  return rtn;
}

// cmd 3
// tcp
int cmd_ca_proto_read( void *buf, e_response_t *r) {
  int rtn;

  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Proto Read\n");
  return rtn;
}

// cmd 4
// tcp
int cmd_ca_proto_write( void *buf, e_response_t *r) {
  int rtn;

  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Proto Write\n");
  return rtn;
}

// cmd 5
// tcp
int cmd_ca_proto_snapshot( void *buf, e_response_t *r) {
  // obsolete
  int rtn;

  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  return rtn;
}

// cmd 6
// tcp and udp
int cmd_ca_proto_search( void *buf, e_response_t *r) {
  int rtn;
  int reply;
  int version;
  int cid;
  int sid;
  char *pl;
  int foundIt;
  e_extended_message_header_t emh;
  e_message_header_t *h1, *h2;
  uint16_t *serverProtocolVersion;

  rtn = read_extended_message_header( buf, &emh);
  pl = buf+rtn;
  pl[emh.plsize-1] = 0;
  rtn += emh.plsize;

  reply   = emh.dtype;
  version = emh.dcount;
  cid     = emh.p1;

  printf( "Search: plsize = %d, version = %d, reply = %d, cid = %d, PV = '%s'\n", emh.plsize, version, reply, cid, pl);

  foundIt = 0;
  for( sid=0; ourpvs[sid].pvname != NULL; sid++) {
    if( strcmp( pl, ourpvs[sid].pvname) == 0) {
      foundIt = 1;
      r->bufsize = 2*sizeof( e_message_header_t) + 8;
      r->buf     = calloc( r->bufsize, 1);
      if( r->buf == NULL) {
	fprintf( stderr, "Out of memory (cmd_ca_proto_search)\n");
	exit( -1);
      }
      h1 = r->buf;
      h2 = h1 + 1;
      create_message_header( h1, 0, 0, 0, 11, 0, 0);		// protocol version
      create_message_header( h2, 6, 8, 5064, 0, sid, cid);	// search response
      serverProtocolVersion  = r->buf + sizeof( e_message_header_t);
      *serverProtocolVersion = htons( 11);
    }
  }

  if( !foundIt && reply == 10) {
    // Docs specify that reply==10 only on a TCP request and that the reply
    // should come back over UDP.  We'll just send the reply back over the same socket
    // it came in on and assume that either the documentation or the protocol are wacky
    //
    r->bufsize = sizeof( e_message_header_t);
    r->buf     = calloc( r->bufsize, 1);
    create_message_header( (e_message_header_t *)r->buf, 14, 0, 10, version, cid, cid);
  }
  return rtn;
}


// cmd 7
// tcp
int cmd_ca_proto_build( void *buf, e_response_t *r) {
  // obsolete
  int rtn;

  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  return rtn;
}

// cmd 8
// tcp
int cmd_ca_proto_events_off( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Events off\n");

  return rtn;
}

// cmd 9
// tcp
int cmd_ca_proto_events_on( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Events on\n");

  return rtn;
}

// cmd 10
// tcp
int cmd_ca_proto_read_sync( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Read Sync\n");

  return rtn;
}

// cmd 11
// tcp
int cmd_ca_proto_error( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Proto Error\n");

  return rtn;
}

// cmd 12
// tcp
int cmd_ca_proto_clear_channel( void *buf, e_response_t *r) {
  //
  int rtn;
  uint32_t sid, cid;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  sid = emh.p1;
  cid = emh.p2;
  printf( "Clear channel sid: %d, cid: %d\n", sid, cid);

  return rtn;
}

// cmd 13
// udp
int cmd_ca_proto_rsrv_is_up( void *buf, e_response_t *r) {
  //
  int rtn;
  uint32_t beaconid;
  struct in_addr addr;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  beaconid = emh.p1;
  addr.s_addr     = htonl(emh.p2);
  printf( "Beacon from %s with id %d\n", inet_ntoa( addr), beaconid);

  return rtn;
}

// cmd 14
// tcp and udp
int cmd_ca_proto_not_found( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Not Found\n");

  return rtn;
}

// cmd 15
// tcp
int cmd_ca_proto_read_notify( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Read Notify\n");

  return rtn;
}

// cmd 16
// tcp
int cmd_ca_proto_read_build( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Read Build\n");

  return rtn;
}

// cmd 17
// udp
int cmd_ca_repeater_confirm( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Repeater Confirm\n");

  return rtn;
}

// cmd 18
// tcp
int cmd_ca_proto_create_chan( void *buf, e_response_t *r) {
  //
  int rtn;
  uint32_t cid;
  uint32_t version;
  uint32_t sid;
  char *pvname;
  char *payload;
  e_extended_message_header_t emh;
  e_message_header *h1, *h2, *h3;

  rtn = read_extended_message_header( buf, &emh);
  payload = buf + rtn;		// pointer to our string
  payload[emh.plsize-1] = 0;	// ensure it is null terminated
  rtn += emh.plsize;
  cid = emh->p1;
  version = emh->p2;

  printf( "Create Chan with name '%s'\n", payload);

  for( sid=0; ourpvs[sid].pvname != NULL; sid++) {
    if( strcmp( payload, ourpvs[sid].pvname) == 0) {

      r->bufsize = 3*sizeof( e_message_header_t);
      r->buf     = calloc( r->bufsize, 1);
      if( r->buf == NULL) {
	fprintf( stderr, "Out of memory (cmd_ca_proto_create_chan)\n");
	exit( -1);
      }

      h1 = (e_message_header_t *)r->buf;
      h2 = (e_message_header_t *)(r->buf + sizeof( e_message_header_t));
      h3 = (e_message_header_t *)(r->buf + 2*sizeof( e_message_header_t));

      create_message_header( h1,  0, 0, 0, 11,   0,   0);	// protocol response
      create_message_header( h2, 22, 0, 0,  0, cid,   3);	// grant read (1) and write (2) access
      create_message_header( h3, 18, 0, 0,  1, cid, sid);	// channel create response
    }
  }
  return rtn;
}

// cmd 19
// tcp
int cmd_ca_proto_write_notify( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Write Notify\n");

  return rtn;
}

// cmd 20
// tcp
int cmd_ca_proto_client_name( void *buf, e_response_t *r) {
  //
  int rtn;
  char *clientName;
  e_extended_message_header_t emh;

  rtn = read_extended_message_header( buf, &emh);
  clientName = buf + rtn;
  clientName[emh.plsize - 1] = 0;
  rtn += emh.plsize;
  printf( "Client Name '%s'\n", clientName);
  //
  // no response
  //
  return rtn;
}

// cmd 21
// tcp
int cmd_ca_proto_host_name( void *buf, e_response_t *r) {
  //
  int rtn;
  char *hostName;
  e_extended_message_header_t emh;

  rtn = read_extended_message_header( buf, &emh);
  hostName = buf + rtn;
  hostName[emh.plsize - 1] = 0;
  rtn += emh.plsize;

  printf( "Host Name '%s'\n", hostName);
  //
  // no response
  //
  return rtn;
}

// cmd 22
// tcp
int cmd_ca_proto_access_rights( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Access Rights\n");

  return rtn;
}

// cmd 23
// tcp and udp
int cmd_ca_proto_echo( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Echo\n");

  return rtn;
}

// cmd 24
// udp
int cmd_ca_repeater_register( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Repeater Register\n");

  return rtn;
}

// cmd 25
// tcp
int cmd_ca_proto_signal( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Signal\n");

  return rtn;
}

// cmd 26
// tcp
int cmd_ca_proto_create_ch_fail( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Create ch fail\n");

  return rtn;
}

// cmd 27
// tcp
int cmd_ca_proto_server_disconn( void *buf, e_response_t *r) {
  //
  int rtn;
  e_extended_message_header_t emh;
  rtn = read_extended_message_header( buf, &emh);
  rtn += emh.plsize;
  printf( "Server Disconnect\n");

  return rtn;
}



int (*cmds[])(void *, e_response_t *) = {
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


void udp_service( struct pollfd *pfd) {
  static struct sockaddr_in fromaddr;	// client's address
  static int fromlen;			// used and ignored to store length of client address
  static char buf[4096], *bp;		// our udp buffer
  static e_response_t ert[1024];	// our responses
  int nert;				// number of responses
  int i;				// loop over responses
  int rsize;				// size of reply packet
  void *reply;				// our reply
  void *rp;				// pointer to next location to write into reply
  int cmd;				// our current command
  int nprocessed;			// number of buffer bytes processed
  int nread;				// number of bytes read
  
  fromlen = sizeof( fromaddr);
  nread = recvfrom( pfd->fd, buf, sizeof( buf), 0, (struct sockaddr *) &fromaddr, &fromlen);

  printf( "From %s port %d read %d bytes\n", inet_ntoa( fromaddr.sin_addr), ntohs(fromaddr.sin_port), nread);

  bp   = buf;
  nert = 0;
  nprocessed = 0;
  while( nprocessed < nread) {
    bp = buf + nprocessed;
    cmd = get_command( bp);
    if( cmd <0 || cmd > 27) {
      fprintf( stderr, "unsupported command %d\n", cmd);
      // stop here: probably should continue but we already have a protocol violation
      break;
    } else {
      printf( "Cmd %d: ", cmd);
      ert[nert].bufsize = 0;
      ert[nert].buf     = NULL;
      nprocessed += cmds[cmd](bp, (ert+nert));
      if( ert[nert].bufsize >= 0) {
	// save responses for the end
	nert++;
	if( nert > (sizeof( ert) / sizeof( ert[0]))) {
	  // Really, this should not happen in real life since the original udp packet would be too big.
	  // stop now and send out what we have
	  break;
	}
      }
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
	fprintf( stderr, "Out of memory for reply (udp_service)\n");
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

void vclistener_service( struct pollfd *pfd) {
  static struct sockaddr_in fromaddr;	// client's address
  static int fromlen;			// used and ignored to store length of client address
  int newsock;
  
  fromlen = sizeof( fromaddr);
  newsock = accept( pfd->fd, (struct sockaddr *)&fromaddr, &fromlen);
  if( newsock < 0) {
    return;
  }
  if( n_e_socks < n_e_socks_max) {
    e_socks[n_e_socks].fd = newsock;
    e_socks[n_e_socks].events = POLLIN;
    n_e_socks++;
  }
}

void vc_service( struct pollfd *pfd) {
}


int main( int argc, char **argv) {
  static int sock;			// our main socket
  static int vclistener;		// our tcp listener
  static struct sockaddr_in addr;	// our address

  int err;				// error return from bind
  int i;				// loop for poll response
  int nfds;				// number of active file descriptors from poll
  int flags;				// used to set non-blocking io for vclistener

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

  e_socks[n_e_socks].fd     = sock;
  e_socks[n_e_socks].events = POLLIN;
  n_e_socks++;

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
  
  e_socks[n_e_socks].fd     = vclistener;
  e_socks[n_e_socks].events = POLLIN;
  n_e_socks++;


  while( 1) {
    nfds = poll( e_socks, n_e_socks, -1);
    for( i=0; nfds>0 && i<n_e_socks_max; i++) {
      if( e_socks[i].revents) {
	nfds--;
	
	if( e_socks[i].fd == sock) {
	  udp_service( e_socks+i);
	  continue;
	}

	if( e_socks[i].fd == vclistener) {
	  vclistener_service( e_socks+i);
	  continue;
	}

	vc_service( e_socks+i);
      }
    }
  }
  return 0;
}

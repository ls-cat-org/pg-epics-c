/*  \file e.c
 *  \brief LS-CAT postgres KV Pairs to EPICS connector
 *  \date 2013
 *  \author Keith Brister
 *  \copyright All Rights Reserved
 */


#include "e.h"
#define E_LISTEN_IP "10.1.0.12"

struct pollfd e_socks[1024];					//!< array of active sockets needs to be only n_e_socks active sockets
e_socks_buffer_t e_sock_bufs[1024];				//!< read buffer to support these sockets hashed by socket number.
int n_e_socks = 0;						//!< current number of sockets
int n_e_socks_max = sizeof( e_socks)/sizeof( e_socks[0]);	//!< maximum number of sockets
int e_max_socket_number = 0;					//!< used to shorten filling of e_socks

static int beacons;						//!< our beacon socket
static struct sockaddr_in broadcastaddr, ouraddr;		//!< addresses for broadcasts and listening
static int beacon_index;					//!< the index of the beacon socket in the socket array
static struct in_addr ournetmask_addr;
static int ht_n = 0;						//!< number of kvs in the hash table (each has two keys in the ht)
static int ht_max_size = (8 * 1024);				//!< maximum size of the hash table
static uint32_t ht_seq = 0;					//!< the magic sequence number to get all the kvs that have changed since we last checked
static e_kvpair_t *kvpair_list=NULL;				//!< Our list of kvpairs: used only to rebuild hash table

static PGconn *q = NULL;					//!< Our connection to the postgresql server

/** List of statements we'll be calling
 *  saved as prepared statements on the server to cut execution time
 */
char* prepared_statements[] = {
  "prepare set_value (int,text) as select e.set_value($1,$2) as rtn",
  "prepare getkvs (int) as select * from e.getkvs( $1)"
};

/** List of sizes for the various dbr types.
 *  dbr name, structure size, type size
 */
e_dbr_size_t dbr_sizes[] = {
  //
  // Keep in order!!
  //
  { "string",       0, 0},
  { "short",        0, 2},
  { "float",        0, 4},
  { "enum",         0, 2},
  { "char",         0, 1},
  { "long",         0, 4},
  { "double",       0, 8},
  { "sts_string",   4, 0},
  { "sts_short",    4, 2},
  { "sts_float",    4, 4},
  { "sts_enum",     4, 2},
  { "sts_char",     5, 1},
  { "sts_long",     4, 4},
  { "sts_double",   8, 8},
  { "time_string", 12, 0},
  { "time_short",  14, 2},
  { "time_float",  12, 4},
  { "time_enum",   14, 2},
  { "time_char",   15, 1},
  { "time_long",   12, 4},
  { "time_double", 16, 8},
  { "gr_string",    0, 0},
  { "gr_short",    24, 2},
  { "gr_float",    40, 4},
  { "gr_enum",    422, 2},
  { "gr_char",     19, 1},
  { "gr_long",     36, 4},
  { "gr_double",   64, 8},
  { "crtl_string",  0, 0},
  { "crtl_short",  28, 2},
  { "crtl_float",  48, 4},
  { "crtl_enum",  422, 2},
  { "crtl_char",   21, 1},
  { "crtl_long",   44, 4},
  { "crtl_double", 80, 8},
  { "unknown_35",   0, 0},
  { "unknown_36",   0, 0},
  { "stack_string", 0, 0},
  { "class_name",   0, 0}
};

e_socks_buffer_t *find_e_socks_buffer( int sock) {
  int buf_index;
  int i;
  for( i=0; i<n_e_socks_max; i++) {
    buf_index = (sock+i) % n_e_socks_max;
    if( e_sock_bufs[buf_index].sock == sock)
      break;
  }
  if( i > n_e_socks_max) {
    fprintf( stderr, "find_e_socks_buffer: could not find buffer for socket %d\n", sock);
    return NULL;
  }
  return e_sock_bufs + buf_index;
}



/** See if this is a .DESC field
 */
int is_desc( char *p) {
  char *q, *q1, *q2;
  int rtn;
    
  rtn = 0;
  q = strdup( p);

  // Find the end of the string
  for( q2 = q; q2 && *q2; q2++);

  // Reverse it
  for( q1 = q, q2--; q2 > q1; q2--, q1++)
    *q1 ^= *q2,
      *q2 ^= *q1,
      *q1 ^= *q2;

  if( strstr( q, "CSED.") == q) {
    rtn = 1;
  }
  free( q);
  return rtn;
}




/** Debugging packet helper
 *  \param n  Number of characters to spew
 *  \param s  The buffer to disgorge
 */
void hex_dump( int n, char *ss) {
  int i,j, n2;
  unsigned char *s;

  n2 = n;
  s = (unsigned char *)ss;

  for( i=0; n > 0; i++) {
    for( j=0; j<16 && n > 0; j++) {
      if( j==8)
	fprintf( stderr, "  ");
      fprintf( stderr, " %02x", *(s + 16*i + j));
      n--;
    }
    fprintf( stderr, "     ");

    for( j=0; j<16 && n2 > 0; j++) {
      if( j==8)
	fprintf( stderr, " ");
      fprintf( stderr, "%c", isprint( *(s + 16*i + j)) ? *(s + 16*i + j) : '.');
      n2--;
    }
    fprintf( stderr, "\n");
  }
  fprintf( stderr, "\n");
}


/** Initialize the socket buffer for the given socket
 */
int e_socks_buf_init( int sock) {
  int i, j;

  fprintf( stderr, "initializing socket %d max of %d\n", sock, n_e_socks_max);
  for( i=0; i<n_e_socks_max; i++) {
    //
    // This tries make the index the same as the socket number
    // A poor man's hash table.
    //
    j = (i+sock) % n_e_socks_max;
    //
    // Found a free one
    //
    if( e_sock_bufs[j].sock == sock || e_sock_bufs[j].sock == -1) {
      if( e_sock_bufs[j].buf != NULL) {
	free( e_sock_bufs[j].buf);
	e_sock_bufs[j].buf = NULL;
	e_sock_bufs[j].bufsize = 0;
      }
      break;
    }
  }

  if( i == n_e_socks_max) {
    // No more room
    fprintf( stderr, "e_socks_buf_init: out of room for more sockets\n");
    return -1;
  }

  e_sock_bufs[j].sock	   = sock;
  e_sock_bufs[j].host_name = NULL;
  e_sock_bufs[j].user_name = NULL;
  e_sock_bufs[j].active    = -1;
  e_sock_bufs[j].events_on = 1;
  e_sock_bufs[j].bufsize   = 4096;
  e_sock_bufs[j].buf       = calloc( e_sock_bufs[j].bufsize, 1);
  if( e_sock_bufs[j].buf == NULL) {
    fprintf( stderr, "out of memory for sock %d (e_socks_buf_init)\n", sock);
  }
  e_sock_bufs[j].rbp       = e_sock_bufs[j].buf;
  e_sock_bufs[j].wbp       = e_sock_bufs[j].buf;
  e_sock_bufs[j].reply_q   = NULL;

  e_max_socket_number = e_max_socket_number < sock ? sock : e_max_socket_number;

  return j;
}

/** Close a socket and associated buffer
 */
void e_socks_buf_close( e_socks_buffer_t *esb) {
  int i;

  esb->active  = 0;

  if( e_max_socket_number <= esb->sock) {
    e_max_socket_number = 0;
    for( i=0; i<n_e_socks_max; i++) {
      if( e_sock_bufs[i].active != 0 )
	  if( e_sock_bufs[i].sock > e_max_socket_number)
	    e_max_socket_number = e_sock_bufs[i].sock;
    }
  }

  if( esb->sock != -1) {
    close( esb->sock);
    esb->sock = -1;
  }
  if( esb->host_name != NULL) {
    free( esb->host_name);
    esb->host_name = NULL;
  }
  if( esb->user_name != NULL) {
    free( esb->user_name);
    esb->user_name = NULL;
  }
  if( esb->bufsize >0 && esb->buf != NULL) {
    free( esb->buf);
  }
  esb->bufsize = 0;
  esb->buf     = NULL;
  esb->rbp     = NULL;
  esb->wbp     = NULL;
  esb->reply_q = NULL;

}




/** Connect to our database server
 */
void pg_conn() {
  PGresult *pgr;
  int wait_interval = 1;
  int connection_init = 0;
  int i;

  if( q == NULL) {
    //
    // make a new conneciton
    //
    q = PQconnectdb( "dbname=ls user=lsuser host=10.1.0.3");
    if( PQstatus(q) != CONNECTION_OK) {
      fprintf( stderr, "Failed to connect to contrabass (pg_conn)\n");
      q = NULL;
      exit( -1);
    }
    connection_init = 1;
  }

  while( PQstatus( q) == CONNECTION_BAD) {
    //
    // Loop forever until a connection can be reestablished
    //
    sleep( wait_interval);
    if( wait_interval < 64)
      wait_interval *= 2;
    connection_init = 1;
  }


  if( connection_init) {
    //
    // Listen for notify, etc.
    //
    pgr = PQexec( q, "select e.init()");
    if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
      fprintf( stderr, "init failed: %s", PQerrorMessage( q));
      exit( -1);
    }
    PQclear( pgr);


    //
    // We use prepared statements except for e.init
    //
    for( i=0; i<sizeof(prepared_statements)/sizeof(prepared_statements[0]); i++) {
      pgr = PQexec( q, prepared_statements[i]);
      if( PQresultStatus( pgr) != PGRES_COMMAND_OK) {
	fprintf( stderr, "Statement preparation failed: %s", PQerrorMessage( q));
	exit( -1);
      }
      PQclear( pgr);
    }

    //
    // We don't need this for IO, just so that poll will work for us
    //
    e_socks_buf_init( PQsocket( q));
  }
}

/** execute a prepared sql statement
 *  A wrapper for PQexePrepared
 *  See http://www.postgresql.org/docs/8.4/static/libpq-exec.html
 *
 *  \param ps           The prepared statement
 *  \param nParams      Number of parameters
 *  \param params       Our array of parameters
 *  \param paramLengths Array of parameter lengths (array can be null if there are no binary formats)
 *  \param paramFormats Array of formats (0 = text, 1 = binary)
 */
PGresult *e_execPrepared( char *ps, int nParams, const char **params, const int *paramLengths, const int *paramFormats, int resultFormat) {
  PGresult *pgr;

  pg_conn();

  pgr = PQexecPrepared( q, ps, nParams, params, paramLengths, paramFormats, resultFormat);
  if( PQresultStatus( pgr) != PGRES_TUPLES_OK) {
    fprintf( stderr, "Statement execution failed: %s", PQerrorMessage( q));
    PQclear( pgr);
    return NULL;
  }
  return pgr;
}


/** swap double to put in into network byte order
 * from http://www.dmh2000.com/cpp/dswap.shtml
 *
 * \param d the double value to scramble
 */
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

/** The inverse of swapd
 *
 * \param a The value to unscramble
 */
double unswapd( long long a) {
  double d;
  unsigned char *dst = (unsigned char *)&d;
  unsigned char *src = (unsigned char *)&a;

  dst[0] = src[7];
  dst[1] = src[6];
  dst[2] = src[5];
  dst[3] = src[4];
  dst[4] = src[3];
  dst[5] = src[2];
  dst[6] = src[1];
  dst[7] = src[0];

  return d;
}

/** see if this is an extended header
 * not used for every command type
 *
 * \param buf The header to check
 */
int get_header_type( char  *buf) {
  // return 1 if extended header, 0 if normal header
  if( *(buf+2)==0xff && *(buf+3)==0xff && *(buf+6)==0 && *(buf+7)==0) {
    return 1;
  }
  return 0;
}

/** return just the command, not the entire header
 * Used to avoid supporting the header type for commands
 * that never, ever, use an extended header.
 *
 * \param buf the header to use
 */
uint16_t get_command( void *buf) {
  uint16_t tmp;
  memcpy( &tmp, buf, sizeof( uint16_t));
  return ntohs( tmp);
}


/** stuff the message header with the correct endian version of the parameters
 *
 * \param mh     pointer to the header to use
 * \param cmd    the command
 * \param plsize payload size
 * \param dtype  data type
 * \param dcount data item count
 * \param p1     Parameter 1
 * \param p2     Parameter 2
 */
void create_message_header( e_message_header_t *mh, uint16_t cmd, uint16_t plsize, uint16_t dtype, uint16_t dcount, uint32_t p1, uint32_t p2) {
  mh->cmd    = htons( cmd);
  mh->plsize = htons( plsize);
  mh->dtype  = htons( dtype);
  mh->dcount = htons( dcount);
  mh->p1     = htonl( p1);
  mh->p2     = htonl( p2);
}

/** set up an extended message header
 *
 * \param emh    Our extended message header
 * \param cmd    The command
 * \param plsize Payload size
 * \param dtype  Data type
 * \param dcount Number of data items
 * \param p1     Parameter 1
 * \param p2     Parameter 2
 */
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


/** Creates a message using either the normal message header or the extended message header, as appropriate.
 *  Calloc's room for the entire payload and returns a pointer to start of the payload memory.
 *
 * Returns pointer to the payload data
 *
 * \param r pointer to the response structure
 * \param cmd      the command
 * \param pllength Payload length
 * \param dtype    type of the data
 * \param dcount   number of data items
 * \param p1       parameter 1
 * \param p2       parameter 2
 */
void *create_message( e_response_t *r, uint16_t cmd, uint32_t pllength, uint16_t dtype, uint32_t dcount, uint32_t p1, uint32_t p2) {
  uint32_t plsize;
  void *rtn;

  //
  // Make sure we the length is divisible by 8
  //
  if( pllength % 8 == 0) {
    plsize = pllength;
  } else {
    plsize = pllength + (8 - (pllength % 8));
  }

  //  fprintf( stderr, "create_message   plsize: %d, dcount: %d\n", plsize, dcount);

  if( plsize > 0x4000) {
    r->bufsize = sizeof( e_extended_message_header_t) + plsize;
    r->buf = calloc( r->bufsize, 1);
    if( r->buf == NULL) {
      fprintf( stderr, "Out of memory (create_message)\n");
      return NULL;
    }

    create_extended_message_header( (e_extended_message_header_t *) r->buf, cmd, plsize, dtype, dcount, p1, p2);
    rtn = r->buf + sizeof( e_extended_message_header_t);

  } else {

    r->bufsize = sizeof( e_message_header_t) + plsize;
    r->buf = calloc( r->bufsize, 1);
    if( r->buf == NULL) {
      fprintf( stderr, "Out of memory (create_message)\n");
      return NULL;
    }
    
    create_message_header( (e_message_header_t *) r->buf, cmd, plsize, dtype, dcount, p1, p2);
    rtn = r->buf + sizeof( e_message_header_t);

  }
  //  fprintf( stderr, "create_message hex dump:\n");
  //  hex_dump( r->bufsize, r->buf);
  return rtn;
}


/** Set up a dbr structure
 *  Basically fills in a structure by "hand"
 *
 * \param pp           Pointer to the space reserved for this structure
 * \param dtype        The type of the structure
 * \param eepoch       Our timestamp
 * \param highlimit    High limit value
 * \param lowlimit     Low limit value
 * \param highlimithit Indicates the high limit has been reached
 * \param lowlimithit  Indicates the low limit has been reached
 * \param prec         The precision of the value contained in this packet
 */
void mk_dbr_struct( void *pp, int dtype, uint32_t eepoch, uint32_t ensec, char *highlimit, char *lowlimit, int highlimithit, int lowlimithit, int prec) {
  struct timeval tv;
  struct timezone tz;
  uint16_t *statusp;
  uint16_t *severityp;
  uint32_t *secPastEpochp;
  uint32_t *nsecp;
  

  //
  // base items have no structure in set
  //
  if( dtype < 7)
    return;

  //  fprintf( stderr, "highlimit %s   lowlimit %s  highlimithit %d  lowlimit hit %d  prec %d\n", highlimit, lowlimit, highlimithit, lowlimithit, prec);


  //
  // everyone else gets these:
  //
  statusp    = pp;
  *statusp   = htons(lowlimithit + 2 * highlimithit);
  severityp  = pp+2;
  *severityp = htons(lowlimithit + 2 * highlimithit);

  //
  // STS structure is now done
  //
  if( dtype/7 == 1)
    return;

  //
  // Time stamp structure
  //
  if( dtype/7 == 2) {
    secPastEpochp  = pp+4;
    nsecp          = pp+8;
    if( eepoch == 0) {
      gettimeofday( &tv, &tz);
      *secPastEpochp = htonl(tv.tv_sec - 631152000);
      *nsecp         = htonl(tv.tv_usec * 1000);
    } else {
      *secPastEpochp = htonl( eepoch);
      *nsecp         = htonl( ensec);
    }
    return;
  }

  //
  // Graphic structure
  //
  if( dtype < 35 && dtype/7 == 3) {
    switch( dtype % 7) {
    case 0:	// string stuff (Really?)  Ignore
      break;
    case 1:	// Integer
      {
	char *units_p;
	int *lower_disp_limit_p;
	int *upper_alarm_limit_p;
	int *upper_warning_limit_p;
	int *lower_warning_limit_p;
	int *lower_alarm_limit_p;
	int *upper_disp_limit_p;
	
	units_p               = pp +  4;
	upper_disp_limit_p    = pp + 12;  *upper_disp_limit_p = htons(atoi( highlimit));
	lower_disp_limit_p    = pp + 14;  *lower_disp_limit_p = htons(atoi( lowlimit));
	upper_alarm_limit_p   = pp + 16;  *upper_alarm_limit_p   = htons(atoi( highlimit));
	upper_warning_limit_p = pp + 18;  *upper_warning_limit_p = htons(atoi( highlimit));
	lower_warning_limit_p = pp + 20;  *lower_warning_limit_p = htons(atoi( lowlimit));
	lower_alarm_limit_p   = pp + 22;  *lower_alarm_limit_p   = htons(atoi( lowlimit));
      }
      break;

    case 2:	// Float (32 bit)
      {
	char *units_p;
	float ftmp;
	int32_t itmp;
	int16_t *prec_p;
	int32_t *upper_disp_limit_p;
        int32_t *lower_disp_limit_p;
	int32_t *upper_alarm_limit_p;
	int32_t *upper_warning_limit_p;
	int32_t *lower_warning_limit_p;
	int32_t *lower_alarm_limit_p;

	

	prec_p                = pp +  4;   *prec_p = htons( prec);
	units_p               = pp +  8;

	upper_disp_limit_p    = pp + 16;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_disp_limit_p = htonl( itmp);

	lower_disp_limit_p    = pp + 20;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_disp_limit_p = htonl( itmp);

	upper_alarm_limit_p   = pp + 24;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_alarm_limit_p   = htonl( itmp);

	upper_warning_limit_p = pp + 28;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_warning_limit_p = htonl( itmp);

	lower_warning_limit_p = pp + 32;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_warning_limit_p = htonl( itmp);

	lower_alarm_limit_p   = pp + 36;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_alarm_limit_p   = htonl( itmp);
      }
      break;

    case 3:	// Enum
      break;

    case 4:	// Char
      {
	char *units_p;
	char *upper_disp_limit_p;
        char *lower_disp_limit_p;
	char *upper_alarm_limit_p;
	char *upper_warning_limit_p;
	char *lower_warning_limit_p;
	char *lower_alarm_limit_p;
	
	units_p               = pp +  4;
	upper_disp_limit_p    = pp + 12;  *upper_disp_limit_p = atoi( highlimit);
	lower_disp_limit_p    = pp + 13;  *lower_disp_limit_p = atoi( lowlimit);
	upper_alarm_limit_p   = pp + 14;  *upper_alarm_limit_p   = atoi( highlimit);
	upper_warning_limit_p = pp + 15;  *upper_warning_limit_p = atoi( highlimit);
	lower_warning_limit_p = pp + 16;  *lower_warning_limit_p = atoi( lowlimit);
	lower_alarm_limit_p   = pp + 17;  *lower_alarm_limit_p   = atoi( lowlimit);
      }
      break;

    case 5:	// Int (32 bit)
      {
	char *units_p;
	int32_t *upper_disp_limit_p;
        int32_t *lower_disp_limit_p;
	int32_t *upper_alarm_limit_p;
	int32_t *upper_warning_limit_p;
	int32_t *lower_warning_limit_p;
	int32_t *lower_alarm_limit_p;
	
	units_p               = pp +  4;
	upper_disp_limit_p    = pp + 12;  *upper_disp_limit_p = htonl(atoi( highlimit));
	lower_disp_limit_p    = pp + 16;  *lower_disp_limit_p = htonl(atoi( lowlimit));
	upper_alarm_limit_p   = pp + 20;  *upper_alarm_limit_p   = htonl(atoi( highlimit));
	upper_warning_limit_p = pp + 24;  *upper_warning_limit_p = htonl(atoi( highlimit));
	lower_warning_limit_p = pp + 28;  *lower_warning_limit_p = htonl(atoi( lowlimit));
	lower_alarm_limit_p   = pp + 32;  *lower_alarm_limit_p   = htonl(atoi( lowlimit));
      }
      break;
      
    case 6:	// Double (64 bit)
      {
	
	uint16_t *prec_p;
	char *units_p;
	double dtmp;
	long long *upper_disp_limit_p;
	long long *lower_disp_limit_p;
	long long *upper_alarm_limit_p;
	long long *upper_warning_limit_p;
	long long *lower_warning_limit_p;
	long long *lower_alarm_limit_p;
	
	prec_p                = pp +  4;   *prec_p = htons( prec);
	units_p               = pp +  8;

	upper_disp_limit_p    = pp + 16;
	dtmp = atof( highlimit);
	*upper_disp_limit_p   = swapd( dtmp);

	lower_disp_limit_p    = pp + 24;
	dtmp = atof( lowlimit);
	*lower_disp_limit_p   = swapd( dtmp);

	upper_alarm_limit_p   = pp + 32;
	dtmp = atof( highlimit);
	*upper_alarm_limit_p   = swapd( dtmp);

	upper_warning_limit_p = pp + 40;
	dtmp = atof( highlimit);
	*upper_warning_limit_p   = swapd( dtmp);

	lower_warning_limit_p = pp + 48;
	dtmp = atof( lowlimit);
	*lower_warning_limit_p   = swapd( dtmp);

	lower_alarm_limit_p   = pp + 56;
	dtmp = atof( lowlimit);
	*lower_alarm_limit_p   = swapd( dtmp);
      }
    }
  }

  //
  // Control structure
  //
  if( dtype < 35 && dtype/7 == 4) {
    switch( dtype % 7) {
    case 0:	// string stuff (Really?)  Ignore
      break;
    case 1:	// Integer
      {
	char *units_p;
	int *lower_disp_limit_p;
	int *upper_alarm_limit_p;
	int *upper_warning_limit_p;
	int *lower_warning_limit_p;
	int *lower_alarm_limit_p;
	int *upper_disp_limit_p;
	int *upper_control_limit_p;
	int *lower_control_limit_p;
	
	units_p               = pp +  4;
	upper_disp_limit_p    = pp + 12;  *upper_disp_limit_p = htons(atoi( highlimit));
	lower_disp_limit_p    = pp + 14;  *lower_disp_limit_p = htons(atoi( lowlimit));
	upper_alarm_limit_p   = pp + 16;  *upper_alarm_limit_p   = htons(atoi( highlimit));
	upper_warning_limit_p = pp + 18;  *upper_warning_limit_p = htons(atoi( highlimit));
	lower_warning_limit_p = pp + 20;  *lower_warning_limit_p = htons(atoi( lowlimit));
	lower_alarm_limit_p   = pp + 22;  *lower_alarm_limit_p   = htons(atoi( lowlimit));
	upper_control_limit_p = pp + 24;  *upper_control_limit_p = htons(atoi( highlimit));
	lower_control_limit_p = pp + 26;  *lower_control_limit_p = htons(atoi( lowlimit));
      }
      break;

    case 2:	// Float (32 bit)
      {
	char *units_p;
	float ftmp;
	int32_t itmp;
	int16_t *prec_p;
	int32_t *upper_disp_limit_p;
        int32_t *lower_disp_limit_p;
	int32_t *upper_alarm_limit_p;
	int32_t *upper_warning_limit_p;
	int32_t *lower_warning_limit_p;
	int32_t *lower_alarm_limit_p;
	int32_t *upper_control_limit_p;
	int32_t *lower_control_limit_p;
	

	prec_p                = pp +  4;   *prec_p = htons( prec);
	units_p               = pp +  8;

	upper_disp_limit_p    = pp + 16;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_disp_limit_p = htonl( itmp);

	lower_disp_limit_p    = pp + 20;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_disp_limit_p = htonl( itmp);

	upper_alarm_limit_p   = pp + 24;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_alarm_limit_p   = htonl( itmp);

	upper_warning_limit_p = pp + 28;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_warning_limit_p = htonl( itmp);

	lower_warning_limit_p = pp + 32;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_warning_limit_p = htonl( itmp);

	lower_alarm_limit_p   = pp + 36;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_alarm_limit_p   = htonl( itmp);

	upper_control_limit_p    = pp + 40;
	ftmp                  = atof( highlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*upper_control_limit_p = htonl( itmp);

	lower_control_limit_p    = pp + 44;
	ftmp                  = atof( lowlimit);  	memcpy( &itmp, &ftmp, sizeof( itmp));
	*lower_control_limit_p = htonl( itmp);

      }
      break;

    case 3:	// Enum
      break;

    case 4:	// Char
      {
	char *units_p;
	char *upper_disp_limit_p;
        char *lower_disp_limit_p;
	char *upper_alarm_limit_p;
	char *upper_warning_limit_p;
	char *lower_warning_limit_p;
	char *lower_alarm_limit_p;
	char *upper_control_limit_p;
        char *lower_control_limit_p;
	
	units_p               = pp +  4;
	upper_disp_limit_p    = pp + 12;  *upper_disp_limit_p = atoi( highlimit);
	lower_disp_limit_p    = pp + 13;  *lower_disp_limit_p = atoi( lowlimit);
	upper_alarm_limit_p   = pp + 14;  *upper_alarm_limit_p   = atoi( highlimit);
	upper_warning_limit_p = pp + 15;  *upper_warning_limit_p = atoi( highlimit);
	lower_warning_limit_p = pp + 16;  *lower_warning_limit_p = atoi( lowlimit);
	lower_alarm_limit_p   = pp + 17;  *lower_alarm_limit_p   = atoi( lowlimit);
	upper_control_limit_p = pp + 18;  *upper_control_limit_p = atoi( highlimit);
	lower_control_limit_p = pp + 19;  *lower_control_limit_p = atoi( lowlimit);
      }
      break;

    case 5:	// Int (32 bit)
      {
	char *units_p;
	int32_t *upper_disp_limit_p;
        int32_t *lower_disp_limit_p;
	int32_t *upper_alarm_limit_p;
	int32_t *upper_warning_limit_p;
	int32_t *lower_warning_limit_p;
	int32_t *lower_alarm_limit_p;
	int32_t *upper_control_limit_p;
        int32_t *lower_control_limit_p;
	
	units_p               = pp +  4;
	upper_disp_limit_p    = pp + 12;  *upper_disp_limit_p = htonl(atoi( highlimit));
	lower_disp_limit_p    = pp + 16;  *lower_disp_limit_p = htonl(atoi( lowlimit));
	upper_alarm_limit_p   = pp + 20;  *upper_alarm_limit_p   = htonl(atoi( highlimit));
	upper_warning_limit_p = pp + 24;  *upper_warning_limit_p = htonl(atoi( highlimit));
	lower_warning_limit_p = pp + 28;  *lower_warning_limit_p = htonl(atoi( lowlimit));
	lower_alarm_limit_p   = pp + 32;  *lower_alarm_limit_p   = htonl(atoi( lowlimit));
	upper_control_limit_p = pp + 36;  *upper_control_limit_p = htonl(atoi( highlimit));
	lower_control_limit_p = pp + 40;  *lower_control_limit_p = htonl(atoi( lowlimit));
      }
      break;
      
    case 6:	// Double (64 bit)
      {
	
	uint16_t *prec_p;
	char *units_p;
	double dtmp;
	long long *upper_disp_limit_p;
	long long *lower_disp_limit_p;
	long long *upper_alarm_limit_p;
	long long *upper_warning_limit_p;
	long long *lower_warning_limit_p;
	long long *lower_alarm_limit_p;
	long long *upper_control_limit_p;
	long long *lower_control_limit_p;
	
	prec_p                = pp +  4;   *prec_p = htons( prec);
	units_p               = pp +  8;

	upper_disp_limit_p    = pp + 16;
	dtmp = atof( highlimit);
	*upper_disp_limit_p   = swapd( dtmp);

	lower_disp_limit_p    = pp + 24;
	dtmp = atof( lowlimit);
	*lower_disp_limit_p   = swapd( dtmp);

	upper_alarm_limit_p   = pp + 32;
	dtmp = atof( highlimit);
	*upper_alarm_limit_p   = swapd( dtmp);

	upper_warning_limit_p = pp + 40;
	dtmp = atof( highlimit);
	*upper_warning_limit_p   = swapd( dtmp);

	lower_warning_limit_p = pp + 48;
	dtmp = atof( lowlimit);
	*lower_warning_limit_p   = swapd( dtmp);

	lower_alarm_limit_p   = pp + 56;
	dtmp = atof( lowlimit);
	*lower_alarm_limit_p   = swapd( dtmp);

	upper_control_limit_p    = pp + 64;
	dtmp = atof( highlimit);
	*upper_control_limit_p   = swapd( dtmp);

	lower_control_limit_p    = pp + 72;
	dtmp = atof( lowlimit);
	*lower_control_limit_p   = swapd( dtmp);

      }
    }
  }
}

/** put the datavalue in the packet
 *
 * \param pp     Our packet
 * \param dtype  The data type
 * \param svalue The value to store in the packet
 */
void pack_dbr_data( void *pp, int dtype, char *svalue) {
  int16_t short_value;
  int32_t int_value, int_value2;
  float   float_value;
  double  double_value;
  long long long_long_value;

  switch( dtype) {
  case  0:	// string
  case  7:
  case 14:
  case 21:
  case 28:
    strcpy( pp, svalue);
    //
    // IF CA clients die when we give them full sized strings then
    // this will need to be changed to
    // strncpy( pp, svalue, MAX_STRING_SIZE-1);
    // Implicit null termination 'cause we used calloc
    //
    break;

  case  1:	// short
  case  8:
  case 15:
  case 22:
  case 29:
    short_value = htons( atoi( svalue));
    memcpy( pp, &short_value, sizeof( short_value));
    break;

  case  2:	// float
  case  9:
  case 16:
  case 23:
  case 30:
    float_value = atof( svalue);
    memcpy( &int_value, &float_value, sizeof( int_value));
    int_value2 = htonl( int_value);
    memcpy( pp, &int_value2, sizeof( int_value2));
    break;

  case  3:	// enum
  case 10:
  case 17:
  case 24:
  case 31:
    short_value = htons( atoi( svalue));
    memcpy( pp, &short_value, sizeof( short_value));
    break;

  case  4:	// char
  case 11:
  case 18:
  case 25:
  case 32:
    //
    // No byte swapping cause it's just one byte
    //
    strcpy( pp, svalue);
    break;

  case  5:	// int
  case 12:
  case 19:
  case 26:
  case 33:
    int_value = htonl( atoi( svalue));
    memcpy( pp, &int_value, sizeof( int_value));
    break;

  case  6:	// double
  case 13:
  case 20:
  case 27:
  case 34:
    double_value = atof( svalue);
    long_long_value = swapd( double_value);
    memcpy( pp, &long_long_value, sizeof( long_long_value));
    break;

  case 37:
    strcpy( pp, "status AOK");
    break;

  case 38:
    strcpy( pp, "px.kvs");
    break;
  }

}


/**
 * \param pgr      result from get_values query
 * \param dbr_type the request return type
 * \param count    the requested count.  If count==0, use the actual return count
 */
void format_dbr( PGresult *pgr, e_response_t *r, int cmd, int dtype, uint32_t dcount, uint32_t p1, uint32_t p2) {
  int
    i,				// loop over query results
    n,				// number of query rows to expect
    return_dcount,		// number of array elements to return (!= n when string as char array is returned)
    struct_size,		// size of the structure before the data array in the payload
    data_size,			// size of the data elements
    eepoch,			// second portion of time stamp
    ensec;			// nano second portion of time stamp
  char *svalue;			// string returned as the value from the query
  char *highlimit;		// our high limit 
  char *lowlimit;		// our low limit 
  void *payload;		// pointer to the packet payload area
  int highlimithit;		// high limit hit
  int lowlimithit;		// low limit hit
  int prec;			// precision for printing
  int highlimithit_col;		// high limit hit
  int lowlimithit_col;		// low limit hit
  int prec_col;			// precision for printing

  //
  // Figure the space required
  //
  if( dtype < 0 || dtype >= sizeof(dbr_sizes)/sizeof(dbr_sizes[0])) {
    fprintf( stderr, "format_dbr: received request for dbr type %d.  Odd.  Changing it to zero.\n", dtype);
    dtype=0;
  }
  struct_size = dbr_sizes[dtype].dbr_struct_size;
  data_size   = dbr_sizes[dtype].dbr_type_size;

  //
  // pick off time stamps
  //
  // Relies on ordering of columns.  Probably OK
  //
  eepoch = ntohl( *(uint32_t *)PQgetvalue( pgr, 0, PQfnumber( pgr, "eepoch")));
  ensec  = ntohl( *(uint32_t *)PQgetvalue( pgr, 0, PQfnumber( pgr, "ensec")));

  highlimit    = PQgetvalue( pgr, 0, PQfnumber( pgr, "high_limit"));
  lowlimit     = PQgetvalue( pgr, 0, PQfnumber( pgr, "low_limit"));

  highlimithit_col = PQfnumber( pgr, "high_limit_hit");
  lowlimithit_col  = PQfnumber( pgr, "low_limit_hit");
  prec_col         = PQfnumber( pgr, "prec");

  highlimithit = highlimithit_col != -1 ? ntohl( *(uint32_t *)PQgetvalue( pgr, 0, highlimithit_col))  : 0;
  lowlimithit  = lowlimithit_col  != -1 ? ntohl( *(uint32_t *)PQgetvalue( pgr, 0, lowlimithit_col))   : 0;
  prec         = prec_col         != -1 ? ntohl( *(uint32_t *)PQgetvalue( pgr, 0, prec_col)) : 0;

  switch( dtype) {
  case 37:
    svalue     = "status AOK";
    break;
  case 38:
    svalue     = "px.kvs";
    break;
  default:
    svalue       = PQgetvalue( pgr, 0, 0);
  }


  // Propagate the evil epics fixed length string
  //
  // It appears that at least caget is happy with the actual string length 
  // instead of a fixed string length.  Have not yet tested strings >= 40 characters.
  //
  if( (dtype < 35 && dtype % 7 == 0)  || dtype == 37 || dtype == 38) {
    if( dcount == 1) {
      data_size = strlen( svalue) + 1;
    } else {
      data_size = MAX_STRING_SIZE;
    }
  }

  //
  // dcount 0 we assume means all of them (TODO: check)
  //
  if( dcount == 0 || dcount > PQntuples(pgr)) {
    n = 1;					// no array support yet
    return_dcount = dcount;
  } else {
    if( dtype % 7 == 4 && dtype != 37 && dtype != 38) {
      data_size = strlen( svalue) + 1;
      return_dcount = data_size;
      n = 1;
    } else {
      n = dcount;
      return_dcount = n;
    }
  }


  //
  // create a message
  //
  payload = create_message( r, cmd, struct_size + data_size * return_dcount, dtype, return_dcount, p1, p2);


  //
  // this is where we'd fill in the structure stuff.  leave it zero for now.
  // (you did use calloc, not malloc, right?)
  //
  mk_dbr_struct( payload, dtype, eepoch, ensec, highlimit, lowlimit, highlimithit, lowlimithit, prec);

  payload += struct_size;

  for( i=0; i<n; i++) {
    pack_dbr_data( payload + data_size*i, dtype, PQgetvalue( pgr, i, 0));
  }  

  //  fprintf( stderr, "format_dbr hex dump:\n");
  //  hex_dump( r->bufsize, r->buf);
}

/**
 * \param pgr      result from get_values query
 * \param dbr_type the request return type
 * \param count    the requested count.  If count==0, use the actual return count
 */
void format_kvp( e_kvpair_t *kvpp, e_response_t *r, int cmd, int dtype, uint32_t dcount, uint32_t p1, uint32_t p2) {
  int
    i,				// loop over query results
    n,				// number of query rows to expect
    return_dcount,		// number of array elements to return (!= n when string as char array is returned)
    struct_size,		// size of the structure before the data array in the payload
    data_size,			// size of the data elements
    eepoch,			// second portion of time stamp
    ensec;			// nano second portion of time stamp
  char *svalue;			// string returned as the value from the query
  char *highlimit;		// our high limit 
  char *lowlimit;		// our low limit 
  void *payload;		// pointer to the packet payload area
  int highlimithit;		// high limit hit
  int lowlimithit;		// low limit hit
  int prec;			// precision for printing


  //
  // Figure the space required
  //
  if( dtype < 0 || dtype >= sizeof(dbr_sizes)/sizeof(dbr_sizes[0])) {
    fprintf( stderr, "format_kvp: received request for dbr type %d.  Odd.  Changing it to zero.\n", dtype);
    dtype=0;
  }
  struct_size = dbr_sizes[dtype].dbr_struct_size;
  data_size   = dbr_sizes[dtype].dbr_type_size;

  //
  // pick off time stamps
  //
  // Relies on ordering of columns.  Probably OK
  //
  eepoch = kvpp->eepoch_secs;
  ensec  = kvpp->eepoch_nsecs;

  highlimit    = kvpp->high_limit     == NULL ? "0.0" : kvpp->high_limit->kvvalue;
  highlimithit = kvpp->high_limit_hit == NULL ? 0     : strtol(kvpp->high_limit_hit->kvvalue, NULL, 10);
  lowlimit     = kvpp->low_limit      == NULL ? "0.0" : kvpp->low_limit->kvvalue;
  lowlimithit  = kvpp->low_limit_hit  == NULL ? 0     : strtol(kvpp->low_limit->kvvalue, NULL, 10);
  prec         = kvpp->prec           == NULL ? 0     : strtol( kvpp->prec->kvvalue, NULL, 10);

  switch( dtype) {
  case 37:
    svalue     = "status AOK";
    break;
  case 38:
    svalue     = "px.kvs";
    break;
  default:
    svalue       = kvpp->array_length > 0 && kvpp->array != NULL ?
      kvpp->array->position->kvvalue :
    kvpp->kvvalue;
  }


  // Propagate the evil epics fixed length string
  //
  // It appears that at least caget is happy with the actual string length 
  // instead of a fixed string length.  Have not yet tested strings >= 40 characters.
  //
  if( (dtype < 35 && dtype % 7 == 0)  || dtype == 37 || dtype == 38) {
    if( dcount == 1) {
      data_size = strlen( svalue) + 1;
    } else {
      data_size = MAX_STRING_SIZE;
    }
  }

  //
  // dcount 0 we assume means all of them (TODO: check)
  //
  if( dcount == 0 || dcount > kvpp->array_length) {
    n = kvpp->array_length;				// minimal array support for now
    return_dcount = dcount;
  } else {
    if( dtype % 7 == 4 && dtype != 37 && dtype != 38) {
      data_size = strlen( svalue) + 1;
      return_dcount = data_size;
      n = 1;
    } else {
      n = dcount;
      return_dcount = n;
    }
  }


  //
  // create a message
  //
  payload = create_message( r, cmd, struct_size + data_size * return_dcount, dtype, return_dcount, p1, p2);


  //
  // this is where we'd fill in the structure stuff.  leave it zero for now.
  // (you did use calloc, not malloc, right?)
  //
  mk_dbr_struct( payload, dtype, eepoch, ensec, highlimit, lowlimit, highlimithit, lowlimithit, prec);

  payload += struct_size;

  if( kvpp->array_length == 0 || kvpp->array == NULL) {
    pack_dbr_data( payload, dtype, kvpp->kvvalue);
  } else {
    e_array_t *arrayp;


    for( arrayp = kvpp->array, i=0; i<n; arrayp = arrayp->next, i++) {
      pack_dbr_data( payload + data_size*i, dtype, arrayp->position == NULL ? kvpp->kvvalue : arrayp->position->kvvalue);
    }
  }

  //  fprintf( stderr, "format_kvp hex dump:\n");
  //  hex_dump( r->bufsize, r->buf);
}

/** Convert a header into our native byte order
 * normal header: meaning of fields is command dependent (should be a union, perhaps)
 *
 * \param inbuf  The incoming buffer to convert
 * \param h      The outgoing buffer to return
 */
void read_message_header( e_socks_buffer_t *inbuf, e_message_header_t *h) {
  memcpy( h, inbuf->rbp, sizeof( struct e_message_header));
  inbuf->rbp += sizeof( struct e_message_header);

  h->cmd    = ntohs( h->cmd);
  h->plsize = ntohs( h->plsize);
  h->dtype  = ntohs( h->dtype);
  h->dcount = ntohs( h->dcount);
  h->p1     = ntohl( h->p1);
  h->p2     = ntohl( h->p2);

  inbuf->payload = inbuf->rbp;
  inbuf->rbp    += h->plsize;

}

/** Convert an extended header to native byte order
 * extended header: meaning of fields is command dependent
 *
 * This routine converts a regular header into an extended header so other functions
 * need only support an extended header
 *
 * \param inbuf  The incomming buffer to conver
 * \param h      The buffer to return
 */
void read_extended_message_header( e_socks_buffer_t *inbuf) {
  struct e_message_header mh;
  e_extended_message_header_t *h;

  h = &(inbuf->emh);

  if( get_header_type( inbuf->rbp)) {
    memcpy( h, inbuf->rbp, sizeof( struct e_extended_message_header));

    h->cmd    = ntohs( h->cmd);
    h->dtype  = ntohs( h->dtype);
    h->p1     = ntohl( h->p1);
    h->p2     = ntohl( h->p2);
    h->plsize = ntohl( h->plsize);
    h->dcount = ntohl( h->dcount);

    inbuf->rbp     += sizeof( e_extended_message_header_t);
    inbuf->payload  = inbuf->rbp;

    inbuf->rbp += inbuf->emh.plsize;
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

  return;
}


/** make up the reply packet
 *
 * \param inbuf      the buffer this is a reponse to
 * \param rsize      size of the reply packet
 * \param reply      The reply packet
 * \param fromaddrp  Our from address
 * \param fromlen    Length of our from address
 */
void mk_reply( e_socks_buffer_t *inbuf, int rsize, char *reply, struct sockaddr_in *fromaddrp, int fromlen) {
  e_reply_queue_t *our_reply;
  e_reply_queue_t *last_reply;		// points to the last reply in the queue

  our_reply = calloc( sizeof( *our_reply), 1);
  if( our_reply == NULL) {
    fprintf( stderr, "Out of memory for our_reply (mk_reply)\n");
    exit( -1);
  }
  our_reply->next         = NULL;
  our_reply->reply_size   = rsize;
  our_reply->reply_packet = reply;

  our_reply->fromlen      = fromlen;
  if( fromlen > 0) {
    our_reply->fromaddr   = *fromaddrp;
  }

  //
  // Add reply to the end of the queue
  // We'd support packet priorities here, I suppose
  //
  if( inbuf->reply_q == NULL)
    inbuf->reply_q = our_reply;
  else {
    for( last_reply=inbuf->reply_q; last_reply->next != NULL; last_reply=last_reply->next);
    last_reply->next = our_reply;
  }
}


/** Notify channel subscribers
 */
void notify_subscribers( e_kvpair_t *kvpp) {
  e_response_t ert;
  e_channel_t *chans;
  e_subscription_t *subs;
  int struct_size, data_size;
  void *payload;

  //
  // Notify the subscribers
  //
  for( chans = kvpp->chans; chans != NULL; chans = chans->next) {
    for( subs = chans->subs; subs != NULL; subs = subs->next) {
      //
      // create a message

      struct_size = dbr_sizes[subs->dtype].dbr_struct_size;
      data_size   = dbr_sizes[subs->dtype].dbr_type_size;
      if( data_size == 0)
	data_size = strlen( kvpp->kvvalue)+1;

      payload = create_message( &ert, 1, struct_size + data_size, subs->dtype, 1, 1, subs->subid);
      // struct filling would go here if we did it
      mk_dbr_struct( payload, subs->dtype, kvpp->eepoch_secs, kvpp->eepoch_nsecs, "0", "0", 0, 0, 0);
      payload += struct_size;
      pack_dbr_data( payload, subs->dtype, kvpp->kvvalue);

      //    fprintf( stderr, "notify_subscribers hex_dump:\n");
      //    hex_dump( ert.bufsize, ert.buf);

      if( ert.buf != NULL && ert.bufsize > 0) {
	mk_reply( find_e_socks_buffer( chans->sock), ert.bufsize, ert.buf, NULL, 0);
      }
    }
  }
}


/** Exchange client and server protocol version numbers
 *
 *          cmd: 0
 * payload size: 0
 *     priority: Virtual circuit priority
 *      version: The version number
 *     reserved: "must be zero" is specified but it appears to be a counter
 *     reserved: "must be zero"
 *
 * tcp and udp
 *
 * \param inbuf  The buffer received
 * \param r
 */
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

  //  fprintf( stderr, "Protocol version received\n");
}



/** Creates a subscription on a channel
 *
 *            cmd:  1
 *   payload size: 16
 *      data type: desired dbr type
 *     data count: desired number of elements (>=0)
 *            SID: Our channel number
 * subscriptionID: ID client uses to identify this subscription
 *
 * tcp
 *
 * \param inbuf The received buffer
 * \param r     Our response
 */
void cmd_ca_proto_event_add( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint16_t *tmp;
  uint32_t sid;
  uint32_t subid;
  char sid_key[9];
  ENTRY entry_in, *entry_outp;
  e_kvpair_t *kvpp;
  e_channel_t *chans;
  e_subscription_t *subs;

  tmp = (uint16_t *)(inbuf->payload + 12);	// skip 3 obsolete 32 bit float values

  sid   = inbuf->emh.p1;
  subid = inbuf->emh.p2;

  sprintf( sid_key, "%08x", sid);

  entry_in.key = sid_key;

  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp != NULL && entry_outp->data != NULL) {
    kvpp = entry_outp->data;
    for( chans = kvpp->chans; chans != NULL; chans = chans->next) {
      if( chans->sock == inbuf->sock)
	break;
    }
    if( chans == NULL) {
      fprintf( stderr, "cmd_ca_proto_event_add: no channel found for socket %d, subid %d, and sid %d\n", inbuf->sock, subid, sid);
      //
      // Signal a bad channel id
      //
      create_message( r, 1, 0, inbuf->emh.dtype, inbuf->emh.dcount, 51, inbuf->emh.p2);
      return;
    }
    subs = calloc( 1, sizeof( e_subscription_t));
    subs->dtype  = inbuf->emh.dtype;
    subs->dcount = inbuf->emh.dcount;
    subs->mask   = *tmp;
    subs->subid  = subid;
    subs->next   = chans->subs;
    chans->subs  = subs;

    // Response
    //
    //             cmd: 1
    //    payload size: response size
    //       data type: same as the request
    //      data count: same as request
    //     status code: ECA_NORMAL (1) on success
    // Subscription ID: same as request
    //
    format_kvp( kvpp, r, 1, inbuf->emh.dtype, inbuf->emh.dcount, 1, inbuf->emh.p2);

  }
}

/** Clears event subscription
 *
 *            cmd: 2
 *   payload size: 0
 *      data type: Same value sent in the subscribe request (do we check this?)
 *     data count: Same value sent in the subscribe request (Again, do we check this?)
 *            SID: Our channel number
 * SubscriptionID: The client's subscriptino ID
 *
 * tcp
 */
void cmd_ca_proto_event_cancel( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t sid, subid;

  char sid_key[9];
  ENTRY entry_in, *entry_outp;
  e_kvpair_t *kvpp;
  e_channel_t *chans;
  e_subscription_t *subs, *last_sub;
  int foundIt;
  
  sid   = inbuf->emh.p1;
  subid = inbuf->emh.p2;

  sprintf( sid_key, "%08x", sid);
  entry_in.key = sid_key;
  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp == NULL || entry_outp->data == NULL)
    return;

  kvpp = entry_outp->data;
  foundIt = 0;
  for( chans = kvpp->chans; chans != NULL; chans = chans->next) {
    if( chans->sock == inbuf->sock) {
      last_sub = NULL;
      for( subs = chans->subs; subs != NULL; subs = subs->next) {
	if( subs->subid == subid) {
	  if( last_sub == NULL)
	    chans->subs = NULL;
	  else
	    last_sub->next = subs->next;
	  free( subs);
	  foundIt = 1;
	  break;
	}
	last_sub = subs;
      }
      if( foundIt)
	break;
    }
  }
}

/** (depreceitated, unsupported here) Read the value of a chennel
 * cmd 3
 * tcp
 * Deprecated
 * Not supported here
 */
void cmd_ca_proto_read( e_socks_buffer_t *inbuf, e_response_t *r) {

  //  printf( "Proto Read\n");
}

/** Write a new channel value
 *
 *          cmd: 4
 * payload size: size of dbr formatted data
 *    data type: dbr type of the data
 *   data count: number of elemets
 *          SID: server channel identifier
 *         IOID: client's identifer of this request
 *
 * tcp
 */
void cmd_ca_proto_write( e_socks_buffer_t *inbuf, e_response_t *r) {
  void *params[3];
  int param_lengths[3];
  int param_formats[3];
  uint32_t struct_size;
  uint32_t data_size, s_size;
  char *sp;
  PGresult *pgr;
  uint32_t dbr_type;
  uint32_t ioid, sid, nkvkey;
  char s[128];
  char sid_key[9];
  ENTRY entry_in, *entry_outp;
  e_kvpair_t *kvpp;

  sid = inbuf->emh.p1;
  ioid = inbuf->emh.p2;
  inbuf->emh.dcount = 1;	// hold the arrays

  //
  // Look up the kv pair
  //
  sprintf( sid_key, "%08x", sid);
  entry_in.key = sid_key;
  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp == NULL || entry_outp->data == NULL) {
    fprintf( stderr, "cmd_ca_proto_write: could not find server ID %d\n", sid);
    return;
  }
  kvpp   = entry_outp->data;
  nkvkey = htonl(kvpp->kvkey);

  //
  // Discover our data size
  //
  dbr_type = htonl(inbuf->emh.dtype);
  struct_size = dbr_sizes[inbuf->emh.dtype].dbr_struct_size;
  data_size   = dbr_sizes[inbuf->emh.dtype].dbr_type_size;

  //
  // TO DO:
  // add proper array support
  //
  switch( inbuf->emh.dtype) {

  case  0:	// string
  case  7:
  case 14:
  case 21:
  case 28:
    sp = inbuf->payload;
    s_size = strlen( sp)+1;
    printf( "Proto Write:  String %s\n", sp);
    if( inbuf->payload + s_size > inbuf->wbp) {
      fprintf( stderr, "Bad string detected (cmd_ca_proto_write)\n");
      inbuf->rbp = inbuf->wbp;
      return;
    }
    inbuf->payload += s_size + 1;
    break;

  case  1:	// int (16 bit)
  case  8:
  case 15:
  case 22:
  case 29:
    snprintf( s, sizeof( s)-1, "%d", ntohs(*(int16_t *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write:  16 bit int %s\n", s);
    inbuf->payload += 2;
    break;

  case  2:	// float (32 bit)
  case  9:
  case 16:
  case 23:
  case 30:
    {
      uint32_t tmp;
      tmp = ntohs( *(uint32_t *)(inbuf->payload));
      snprintf( s, sizeof( s)-1, "%f", *(float *)&tmp);
      s[sizeof(s)-1] = 0;
      sp = s;
      printf( "Proto Write:  32 bit float %s\n", s);
      inbuf->payload += 4;
    }
    break;

  case  3:	// enum (16 bit unsigned int)
  case 10:
  case 17:
  case 24:
  case 31:
    snprintf( s, sizeof( s)-1, "%u", ntohs(*(uint16_t *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write:  enum: %s\n", s);
    inbuf->payload += 2;
    break;

  case  4:	// enum (8 bit unsigned int)
  case 11:
  case 18:
  case 25:
  case 32:
    snprintf( s, sizeof( s)-1, "%u", *(unsigned char *)(inbuf->payload));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write:  8 bit int %s\n", s);
    inbuf->payload += 1;
    break;

  case  5:	// enum (32 bit signed int)
  case 12:
  case 19:
  case 26:
  case 33:
    snprintf( s, sizeof( s)-1, "%d", ntohl( *(int32_t *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write:  32 bit int %s\n", s);
    inbuf->payload += 4;
    break;

  case  6:	// double (64 bit)
  case 13:
  case 20:
  case 27:
  case 34:
    snprintf( s, sizeof( s)-1, "%f", unswapd( *(long long *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write:  64 bit double %s\n", s);
    inbuf->payload += 8;
    break;
  }


  if( strcmp( kvpp->kvvalue, sp) != 0) {
    if( kvpp->kvvalue != NULL)
      free( kvpp->kvvalue);
    kvpp->kvvalue        = strdup( sp);

    notify_subscribers( kvpp);
    
    params[0] = &nkvkey;	        param_lengths[0] = sizeof(nkvkey);	param_formats[0] = 1;
    params[1] = sp;		param_lengths[1] = 0;			param_formats[1] = 0;
    pgr = e_execPrepared( "set_value", 2, (const char **)params, param_lengths, param_formats, 0);
    if( pgr == NULL)
      return;
    PQclear( pgr);

  }
}

/** Obsolete function unsupported here
 * cmd 5
 * tcp
 * obsolete
 * Not supported here
 */
void cmd_ca_proto_snapshot( e_socks_buffer_t *inbuf, e_response_t *r) {
}

/** Searches for a given channel name
 *
 *          cmd: 6
 * payload size: padded size of channel name
 *        reply: 10 = don't reply on failed search, 5 = should reply on failed search
 *      version: minor protocol version number
 *          CID: client id number
 * (docs specify that CID should be repeated in parameter 2, I'm not sure this is true)
 *
 * tcp and udp
 */
void cmd_ca_proto_search( e_socks_buffer_t *inbuf, e_response_t *r) {

  int reply;
  int version, versionn;
  int cid;
  char *pl;
  int foundIt;
  ENTRY entry_in, *entry_outp;

  pl = inbuf->payload;
  if( inbuf->emh.plsize <= 0) {
    fprintf( stderr, "cmd_ca_proto_search: no channel name requested\n");
    return;
  }
  pl[inbuf->emh.plsize-1] = 0;

  reply    = inbuf->emh.dtype;
  version  = inbuf->emh.dcount;
  versionn = htonl( version);
  cid      = inbuf->emh.p1;

  if( strcmp( "thisIsTheEnd", pl) == 0) {
    exit( 0);
  }

  //
  // Reject requests from other networks
  //
  if( (r->peer.sin_addr.s_addr | ournetmask_addr.s_addr) != ournetmask_addr.s_addr) {
    foundIt = 0;
  } else {

    entry_in.key = pl;
    entry_outp = hsearch( entry_in, FIND);
    if( entry_outp != NULL)
      foundIt = 1;
    else
      foundIt = 0;
  }

  //
  // See if the channel ends in ".DESC"
  // If so, we pretend it exists.  Note that we can always add a DESC field to the DB
  // in which case we've found it already
  //
  if( !foundIt && is_desc( pl))
    foundIt = 1;

  if( foundIt) {
    uint16_t server_protocol_version = 11, *spvp;

    fprintf( stderr, "cmd_ca_proto_search: found channel %s  socket=%d  cid=%d\n", pl, inbuf->sock, cid);
    // Response
    //
    //          cmd: 6
    // payload size: 8
    //  port number: our port number (5064)
    //   data count: 0
    //          SID: 0xffffffff
    //          CID: same as the request
    //
    spvp = create_message( r, 6, 8, 5064, 0, 0xffffffff, cid);
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
    // Response
    //
    //          cmd: 14
    // payload size:  0
    //   reply flag:  DO_REPLY (aka 10)
    //      version:  same as request
    //          CID:  same as request
    //          CID:  same as request
    //
    create_message( r, 14, 0, 10, version, cid, cid);
  }
}


/** Documented only as being obsolete
 * cmd 7
 * tcp
 * obsolete
 * Not supported here
 */
void cmd_ca_proto_build( e_socks_buffer_t *inbuf, e_response_t *r) {
}

/** Disable server from sending subscription updates to this circuit
 *
 *          cmd: 8
 * payload size: 0
 *        dtype: 0
 *       length: 0
 *           p1: 0
 *           p2: 0
 *
 * tcp
 */
void cmd_ca_proto_events_off( e_socks_buffer_t *inbuf, e_response_t *r) {
  inbuf->events_on = 0;
  //  printf( "Events off\n");
}


/** Enable server sending subscriptions updates to this circuit
 *
 *          cmd: 9
 * payload size: 0
 *        dtype: 0
 *       length: 0
 *           p1: 0
 *           p2: 0
 *
 * tcp
 */
void cmd_ca_proto_events_on( e_socks_buffer_t *inbuf, e_response_t *r) {
  inbuf->events_on = 1;
  //  printf( "Events on\n");
}

/** Deprecated and un documented
 *
 * cmd 10
 * tcp
 * Deprecated
 * Not implemented here
 */
void cmd_ca_proto_read_sync( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Read Sync\n");
}

/** Sends error message and code
 *
 * cmd: 11
 * payload size: Size of the request header that triggered the error plus size of the error message.
 *     reserved: "must be zero"
 *     reserved: "must be zero"
 *          CID: Client's id for the failed channel
 *  Status code: an ECA code
 *
 * tcp
 *
 * UNIMPLIMENTED
 */
void cmd_ca_proto_error( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Proto Error\n");
}

/** Clears the channel (shuts it down)
 *
 *           cmd: 12
 *  payload size: 0
 *     data type: 0
 *   data length: 0
 *           SID: Server's channel identifier
 *           CID: Client's channel identifier
 *
 * tcp
 */
void cmd_ca_proto_clear_channel( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t sid, cid;
  ENTRY entry_in, *entry_outp;
  e_kvpair_t *kvpp;
  e_channel_t *chans, *last_chan;
  e_subscription_t *subs, *last_sub;
  char sid_key[9];

  sid = inbuf->emh.p1;
  cid = inbuf->emh.p2;

  sprintf( sid_key, "%08x", sid);
  entry_in.key = sid_key;
  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp == NULL || entry_outp->data == NULL)
    return;

  kvpp = entry_outp->data;

  last_chan = NULL;
  for( chans = kvpp->chans; chans != NULL; chans = chans->next) {
    if( chans->sock == inbuf->sock && chans->cid == cid) {
      last_sub  = NULL;
      for( subs = chans->subs; subs != NULL; subs = subs->next) {
	if( last_sub != NULL)
	  free( last_sub);
	last_sub = subs;
      }
      if( last_sub != NULL)
	free( last_sub);
      break;
    }
    last_chan = chans;
  }
  if( chans != NULL) {
    if( last_chan == NULL)
      kvpp->chans = NULL;
    else
      last_chan->next = chans->next;
    free( chans);
  }

  //
  // The client does nothing with this message: it's just noise.
  //
  // Response
  //
  //          cmd: 12
  // payload size: 0
  //    data type: 0
  //  data length: 0
  //          SID: server id (as sent to us)
  //          CID: client id (as sent to us)
  //

  create_message( r, 12, 0, 0, 0, sid, cid);
  inbuf->active--;
}

/** Beacon sent by server when it becomes available
 *
 *             cmd: 13
 *    payload size:  0
 *     Server port:  TCP at which to find the server
 *        reserved:  0
 *       Beacon ID:  sequential beacon id
 *         Address:  may contain the IP address of the server or may be zero
 *
 * udp
 */
void cmd_ca_proto_rsrv_is_up( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t beaconid;

  struct in_addr addr;

  beaconid = inbuf->emh.p1;

  if( inbuf->emh.p2 == 0) {
    addr = r->peer.sin_addr;
  } else {
    addr.s_addr     = htonl( inbuf->emh.p2);
  }
  // printf( "Beacon from %s with id %d\n", inet_ntoa( addr), beaconid);
}


/** Indicates the requested channel does not exist
 *  Currently unimplement as this is a response command and only
 *  implemented in the client.
 *
 *            cmd: 14
 * payload length:  0
 *     reply flag: 10  (AKA DO_REPLY)
 *        version: same as request
 *            CID: same as request
 *            CID: same as request
 * tcp and udp
 *
 * This command should be implemented when we support operations as a client.
 * As a server, this is possibly sent in response to a failed ca_proto_search request.
 */
void cmd_ca_proto_not_found( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Not Found\n");
}


/** Read the value of a channel
 *
 *          cmd: 15
 * payload size:  0
 *    data type: dbr type
 *   data count: >= 0
 *          SID: server's channel identifier
 *          CID: client's channel identifier
 *
 * tcp
 */
void cmd_ca_proto_read_notify( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t sid;
  uint32_t ioid;
  ENTRY entry_in, *entry_outp;
  char sid_key[9];
  e_kvpair_t *kvpp;

  sid  = inbuf->emh.p1;
  ioid = inbuf->emh.p2;

  sprintf( sid_key, "%08x", sid);
  entry_in.key = sid_key;
  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp == NULL || entry_outp->data == NULL)
    return;

  kvpp = entry_outp->data;

  //
  // Docs say p1 is sid but really it is the error code
  // dbr error code for AOK is 1
  //
  //           cmd: 15
  //  payload size: size of our payload
  //     data type: type of payload (hint: same as request)
  //   data length: number of elements (should be same as request)
  //    error code: 1 for AOK
  //          ioid: Id from client
  //
  format_kvp( kvpp, r, 15, inbuf->emh.dtype, inbuf->emh.dcount, 1, ioid);

  
  

  //  fprintf( stderr, "Read Notify for sid=%d  ioid=%d   dtype=%d\n", sid, ioid, inbuf->emh.dtype);
  //  fprintf( stderr, "read_notify result:\n");
  //  hex_dump( r->bufsize, r->buf);
}

/** Obsolete and undocumented
 *
 * cmd 16
 * tcp
 * Obsolete
 * Not implemented here
 */
void cmd_ca_proto_read_build( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Read Build\n");
}

/** Confirms successful repeater client registration
 *
 *           cmd: 17
 *  payload size:  0
 *     data type:  0
 *   data length:  0
 *   parameter 1:  0
 *   parameter 2:  server ip address
 *
 * udp
 * TODO
 * Implement when we start operating as a repeater
 */
void cmd_ca_repeater_confirm( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Repeater Confirm\n");
}

/** Requests the creation of a channel
 *
 *            cmd: 18
 *   payload size: padded length of the channel name
 *       reserved: 0
 *       reserved: 0
 *            CID: client's channel identifier
 * client version: minor protocol version of the client
 *
 * tcp
 */
void cmd_ca_proto_create_chan( e_socks_buffer_t *inbuf, e_response_t *r) {
  uint32_t cid, cidn;
  uint32_t version, versionn;
  
  e_message_header_t *h1, *h2, *h3;
  ENTRY entry_in, *entry_outp;
  e_kvpair_t       *kvpp;
  e_channel_t      *chans;

  inbuf->payload[inbuf->emh.plsize-1] = 0;	// ensure it is null terminated
  cid = inbuf->emh.p1;
  version = inbuf->emh.p2;


  if( inbuf->host_name == NULL)
    inbuf->host_name = strdup("");
  if( inbuf->user_name == NULL)
    inbuf->user_name = strdup("");
  cidn = htonl( cid);
  versionn = htonl( version);

  fprintf( stderr, "Create Chan with name '%s' for user '%s' on machine '%s' and socket %d with cid %d\n", inbuf->payload, inbuf->user_name, inbuf->host_name, inbuf->sock, cid);

  

  entry_in.key = inbuf->payload;
  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp == NULL && is_desc( inbuf->payload)) {
    char *zz;
    zz = strdup( "epics.DESC");
    entry_in.key = zz;
    entry_outp = hsearch( entry_in, FIND);
    free( zz);
  }

  if( entry_outp != NULL && entry_outp->data != NULL) {
    kvpp = entry_outp->data;
    chans = calloc( 1, sizeof( e_channel_t));
    chans->hostname = strdup( inbuf->host_name);
    chans->username = strdup( inbuf->user_name);
    chans->sock     = inbuf->sock;
    chans->version  = version;
    chans->cid      = cid;
    chans->subs     = NULL;
    chans->next     = kvpp->chans;
    kvpp->chans     = chans;

    r->bufsize = sizeof( *h1) * 3;
    r->buf = calloc( 1, r->bufsize);

    h1 = (e_message_header_t *)r->buf;
    h2 = h1 + 1;
    h3 = h2 + 1;
    //
    // Responses (3, count 'em, 3)
    //
    // the protocol reponse cmd 0, minor version 11
    // access rights cmd 22: read and write
    // and
    //           cmd: 18
    //  payload size:  0
    //     data type: native type
    //    data count: native length
    //           CID: as the client sent us
    //           SID: our channel identifier
    //
    create_message_header( h1,  0, 0, 0, 11,   0,   0);					// protocol response: cmd:0  minor version: 11 (in data length field)
    create_message_header( h2, 22, 0, 0,  0, cid,   3);					// grant read (1) and write (2) access
    create_message_header( h3, 18, 0, kvpp->dbr_type,  kvpp->array_length > 0 ? kvpp->array_length : 1, cid, kvpp->sid);	// channel create response

    if( inbuf->active == -1) {
      inbuf->active = 1;
    } else {
      inbuf->active++;
    }
  } else {
    //
    // Failed to create channel
    //
    //             cmd: 26
    //  payload length: 0
    //       data type: 0
    //     data length: 0
    //             CID: from client
    //     parameter 2: 0
    //
    create_message( r, 26, 0, 0, 0, cid, 0);

    if( inbuf->active == -1) {
      //
      // First attempt after the connection
      // just let it die
      //
      inbuf->active = 0;
    }
  }
}

/** Writes the new channel value
 *
 *          cmd: 19
 * payload size: size of the dbr formatted payload
 *    data type: dbr type of the data
 *   data count: number of elements
 *          SID: server's id for this channel
 *         IOID: client's id for this request
 *
 * tcp
 */
void cmd_ca_proto_write_notify( e_socks_buffer_t *inbuf, e_response_t *r) {
  void *params[3];
  int param_lengths[3];
  int param_formats[3];
  uint32_t struct_size;
  uint32_t data_size;
  PGresult *pgr;
  uint32_t dbr_type;
  uint32_t ioid, sid, nkvkey;
  int rtn;
  char s[128];
  char *sp;
  int s_size;
  int rtn_value;
  ENTRY entry_in, *entry_outp;
  char sid_key[9];
  e_kvpair_t *kvpp;

  
  sid = inbuf->emh.p1;
  ioid = inbuf->emh.p2;
  inbuf->emh.dcount = 1;	// hold the arrays

  sprintf( sid_key, "%08x", sid);
  entry_in.key = sid_key;
  entry_outp = hsearch( entry_in, FIND);

  if( entry_outp == NULL) {
    fprintf( stderr, "cmd_ca_proto_write_notify: could not find server ID %d\n", sid);
    //
    // Response
    //
    //           cmd: 19
    //  payload size:  0
    //     data type: same as request
    //   data length: same as request
    //   status code: ECA_NORMAL (1)  or ECA_PUTFAIL (160)
    //          IOID: from client
    //
    create_message( r, 19, 0, inbuf->emh.dtype, inbuf->emh.dcount, ntohl( 160), ioid);
    return;
  }

  kvpp = entry_outp->data;
  nkvkey = htonl(kvpp->kvkey);
  
  //
  // Discover our data size
  //
  dbr_type = htonl(inbuf->emh.dtype);
  struct_size = dbr_sizes[inbuf->emh.dtype].dbr_struct_size;
  data_size   = dbr_sizes[inbuf->emh.dtype].dbr_type_size;

  rtn = 160;	// default ca put fail
  printf( "Proto Write Notify\n");


  //
  // TO DO:
  // add proper array support
  //
  switch( inbuf->emh.dtype) {

  case  0:	// string
  case  7:
  case 14:
  case 21:
  case 28:
    sp = inbuf->payload;
    s_size = strlen( sp)+1;
    printf( "Proto Write Notify:  String %s\n", sp);
    if( inbuf->payload + s_size > inbuf->wbp) {
      fprintf( stderr, "Bad string detected (cmd_ca_proto_write)\n");
      inbuf->rbp = inbuf->wbp;
      return;
    }
    inbuf->payload += s_size + 1;
    break;

  case  1:	// int (16 bit)
  case  8:
  case 15:
  case 22:
  case 29:
    snprintf( s, sizeof( s)-1, "%d", ntohs(*(int16_t *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write Notify:  16 bit int %s\n", s);
    inbuf->payload += 2;
    break;

  case  2:	// float (32 bit)
  case  9:
  case 16:
  case 23:
  case 30:
    {
      uint32_t tmp;
      tmp = ntohs( *(uint32_t *)(inbuf->payload));
      snprintf( s, sizeof( s)-1, "%f", *(float *)&tmp);
      s[sizeof(s)-1] = 0;
      sp = s;
      printf( "Proto Write Notify:  32 bit float %s\n", s);
      inbuf->payload += 4;
    }
    break;

  case  3:	// enum (16 bit unsigned int)
  case 10:
  case 17:
  case 24:
  case 31:
    snprintf( s, sizeof( s)-1, "%u", ntohs(*(uint16_t *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write Notify:  enum: %s\n", s);
    inbuf->payload += 2;
    break;

  case  4:	// enum (8 bit unsigned int)
  case 11:
  case 18:
  case 25:
  case 32:
    snprintf( s, sizeof( s)-1, "%u", *(unsigned char *)(inbuf->payload));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write Notify:  8 bit int %s\n", s);
    inbuf->payload += 1;
    break;

  case  5:	// enum (32 bit signed int)
  case 12:
  case 19:
  case 26:
  case 33:
    snprintf( s, sizeof( s)-1, "%d", ntohl( *(int32_t *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write Notify:  32 bit int %s\n", s);
    inbuf->payload += 4;
    break;

  case  6:	// double (64 bit)
  case 13:
  case 20:
  case 27:
  case 34:
    snprintf( s, sizeof( s)-1, "%f", unswapd( *(long long *)(inbuf->payload)));
    s[sizeof(s)-1] = 0;
    sp = s;
    printf( "Proto Write Notify:  64 bit double %s\n", s);
    inbuf->payload += 8;
    break;
  }

  if( strcmp( kvpp->kvvalue, sp) != 0) {
    if( kvpp->kvvalue != NULL)
      free( kvpp->kvvalue);
    kvpp->kvvalue        = strdup( sp);
    
    notify_subscribers( kvpp);
  }

  params[0] = &nkvkey;	        param_lengths[0] = sizeof(nkvkey);	param_formats[0] = 1;
  params[1] = sp;		param_lengths[1] = 0;			param_formats[1] = 0;
  pgr = e_execPrepared( "set_value", 2, (const char **)params, param_lengths, param_formats, 0);
  if( pgr == NULL)
    return;
  rtn_value = ntohl( *(uint32_t *)PQgetvalue( pgr, 0, PQfnumber( pgr, "rtn")));
  PQclear( pgr);

  //
  // Response
  //
  //           cmd: 19
  //  payload size:  0
  //     data type: same as request
  //   data length: same as request
  //   status code: ECA_NORMAL (1)  or ECA_PUTFAIL (160)
  //          IOID: from client
  //
  create_message( r, 19, 0, inbuf->emh.dtype, inbuf->emh.dcount, rtn_value, ioid);
}

/** Sends the local username to the server
 *
 *           cmd: 20
 *  payload size: string length of the user name
 *     data type: 0
 *   data length: 0
 *   parameter 1: 0
 *   parameter 2: 0
 *
 * tcp
 */
void cmd_ca_proto_client_name( e_socks_buffer_t *inbuf, e_response_t *r) {
  char *clientName;

  clientName = inbuf->payload;
  clientName[inbuf->emh.plsize - 1] = 0;

  if( inbuf->user_name != NULL) {
    free( inbuf->user_name);
  }
  inbuf->user_name = strdup( clientName);

  fprintf( stderr, "Client Name '%s'\n", clientName);
  //
  // no response
  //
}

/** Sends the local host name to the server
 *
 *          cmd: 21
 * payload size: string length of the hostname
 *    data type: 0
 *  data length: 0
 *  parameter 1: 0
 *  parameter 1: 0
 *
 * tcp
 */
void cmd_ca_proto_host_name( e_socks_buffer_t *inbuf, e_response_t *r) {
  //
  char *hostName;

  hostName = inbuf->payload;
  hostName[inbuf->emh.plsize - 1] = 0;

  if( inbuf->host_name != NULL) {
    free( inbuf->host_name);
  }
  inbuf->host_name = strdup( hostName);

  fprintf( stderr, "Host Name '%s'\n", hostName);
  //
  // no response
  //
}

/** Response command: notify client of access rights
 *  Only needed as a command in the client
 *
 *            cmd: 22
 *   payload size:  0
 *      data type:  0
 * payload length:  0
 *            CID:  client channel identifier
 *  Access Rights: 0 = no rights, 1 = read only, 2 = write only (really?), 3 = read and write
 *
 * tcp
 */
void cmd_ca_proto_access_rights( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Access Rights\n");
}

/** Connection verify
 *
 *          cmd: 23
 * payload size:  0
 *    data type:  0
 *  data length:  0
 *  parameter 1:  0
 *  parameter 2:  0
 *
 * tcp and udp
 */
void cmd_ca_proto_echo( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Echo\n");

  r->bufsize = sizeof( e_message_header_t);
  r->buf = calloc( r->bufsize, 1);
  if( r->buf == NULL) {
    fprintf( stderr, "Out of memory (cmd_ca_proto_echo)\n");
    return;
  }
  // Response
  //
  //          cmd: 23
  // payload size: 0
  //    data type: 0
  //   data count: 0
  //  parameter 1: 0
  //  parameter 2: 0
  //
  create_message( r, 23, 0, 0, 0, 0, 0);
}

/** Requests registration with the repeater
 *  NOT IMPLEMENTED
 *
 * cmd: 24
 * payload size: 0
 *    data type: 0
 *  data length: 0
 *  parameter 1: 0
 *  parameter 2: IP address of client
 *
 * udp
 * TODO
 */
void cmd_ca_repeater_register( e_socks_buffer_t *inbuf, e_response_t *r) {

  //  printf( "Repeater Register\n");
}

/** obsolete and undocumented
 *
 * cmd: 25
 * tcp
 * Obsolete
 * Not implemented here
 */
void cmd_ca_proto_signal( e_socks_buffer_t *inbuf, e_response_t *r) {

  //  printf( "Signal\n");
}

/** Response to a failed channel creation
 *  Not implemented as it is only a command on the client side
 *
 *          cmd: 26
 * payload size:  0
 *    data type:  0
 *  data length:  0
 *          CID:  cient id as in the original request
 *  parameter 2:  0
 *
 * tcp
 * TODO: Implement when we need to act as a client.
 */
void cmd_ca_proto_create_ch_fail( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Create ch fail\n");
}

/** Notifies client that server has disconnected the channel
 *  This is a reponse command and unimplemented here
 *
 *          cmd: 27
 * payload size:  0
 *    data type:  0
 *  data length:  0
 *          CID:  The client channel identifier
 *  parameter 2:  0
 *
 * tcp
 * TODO: Implement when we need to act as a client
 */
void cmd_ca_proto_server_disconn( e_socks_buffer_t *inbuf, e_response_t *r) {
  //  printf( "Server Disconnect\n");
}



/** Command array to access commands in an O(1) manner
 */
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



/** Fix up buffer pointers
 *
 * Take all unread bytes in the buffer
 * and move them to the front.
 */
void fixup_bps( e_socks_buffer_t *b) {
  int nbytes;

  if( b==NULL) {
    fprintf( stderr, "Bad buffer pointer (why?) (fixup_bps)\n");
    return;
  }
  if( b->buf==NULL) {
    fprintf( stderr, "Bad buffer->buf pointer (why?) (fixup_bps)\n");
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


/** Channel Access packet service routine
 *
 * \param pfd   The pollfd structure for this socket
 * \param inbuf Our input buffer
 */
void ca_service( struct pollfd *pfd) {
  static struct sockaddr_in fromaddr;	// client's address
  static unsigned int fromlen;		// used and ignored to store length of client address
  static e_response_t ert[1024];	// our responses
  e_socks_buffer_t *inbuf;		// the buffer to use

  e_extended_message_header_t bad_cmd_header;	// used to skip commands we do not know how to handle
  void *old_rbp;			// used to be sure we are still reading from the buffer
  int nert;				// number of responses
  int i;				// loop over responses
  int rsize;				// size of reply packet
  void *reply;				// our reply
  void *rp;				// pointer to next location to write into reply
  int cmd;				// our current command
  int nread;				// number of bytes read

  
  inbuf = find_e_socks_buffer( pfd->fd);
  if( inbuf == NULL) {
    fprintf( stderr, "ca_service: unknown socket %d\n", pfd->fd);
    return;
  }

  if( pfd->revents & (POLLERR | POLLHUP | POLLNVAL)) {
    //
    // close the socket and ignore it ever more
    //
    inbuf->active = 0;

    return;
  }


  if( pfd->revents & POLLOUT) {
    // Service outgoing packets before incoming ones
    //
    int sent_count;
    e_reply_queue_t *next;

    //    fprintf( stderr, "Here I am in ca_service POLLOUT\n");

    if( inbuf->reply_q != NULL) {
      next = inbuf->reply_q;


      if( next->fromlen != 0) {
	//	fprintf( stderr, "To %s port %d write %d bytes\n", inet_ntoa( next->fromaddr.sin_addr), ntohs( next->fromaddr.sin_port), next->reply_size);
	//	hex_dump( next->reply_size, next->reply_packet);
	sent_count = sendto( pfd->fd, next->reply_packet, next->reply_size, 0, (const struct sockaddr *)&next->fromaddr, next->fromlen);
      } else {
	sent_count = send( pfd->fd, next->reply_packet, next->reply_size, 0);
      }
      if( inbuf->active == 0) {
	e_socks_buf_close( inbuf);
      }
      if( sent_count == -1) {
	fprintf( stderr, "fromlen: %d     fromaddr: %s\n", next->fromlen, inet_ntoa( next->fromaddr.sin_addr));
	perror( "ca_service");
	if( pfd->fd == beacons) {
	  hex_dump( next->reply_size, next->reply_packet);
	} else {
	  inbuf->active = 0;
	  e_socks_buf_close( inbuf);
	}
	inbuf->reply_q = next->next;
	free( next->reply_packet);
	free( next);
	return;
      }

      if( sent_count == 0) {
	fprintf( stderr, "(ca_service) possible bad connect, cutting out\n");
	inbuf->active = 0;
	e_socks_buf_close( inbuf);
	return;
      }

      if( sent_count == next->reply_size) {
	//
	// Done with this packet
	//
	inbuf->reply_q = next->next;
	free( next->reply_packet);
	free( next);
      } else {
	//
	// Still more to send
	// Possibly we are sending a big array or something.
	//
	memcpy( next->reply_packet, next->reply_packet + sent_count, next->reply_size - sent_count);
	next->reply_size -= sent_count;
      }
    }
  }

  if( inbuf->active != 0 && (pfd->revents & POLLIN) != 0) {

    fixup_bps( inbuf);

    fromlen = sizeof( fromaddr);
    nread = recvfrom( pfd->fd, inbuf->wbp, inbuf->bufsize - (inbuf->wbp - inbuf->rbp), 0, (struct sockaddr *) &fromaddr, &fromlen);
    if( nread == -1 || nread == 0) {
      // we should stick some error handling code here
      // for now we assume the UDP listening socket is not going to close on its own
      //
      return;
    }

    // fprintf( stderr, "From %s port %d read %d bytes\n", inet_ntoa( fromaddr.sin_addr), ntohs(fromaddr.sin_port), nread);
    //    hex_dump( nread, inbuf->wbp);

    inbuf->wbp += nread;


    nert = 0;
    while( inbuf->rbp < inbuf->wbp) {


      old_rbp = inbuf->rbp;
      read_extended_message_header( inbuf);
      cmd = inbuf->emh.cmd;

      //      fprintf( stderr, "main: nert = %d, cmd = %d\n", nert, cmd);

      if( cmd <0 || cmd > 27) {
	//
	// Bad command: either a protocol version problem or we have a messed up packet.
	//
	fprintf( stderr, "unsupported command %d with payload size %d\n", cmd, bad_cmd_header.plsize);
	if( inbuf->rbp > inbuf->wbp) {
	  fprintf( stderr, "request to read more bytes than we have: likely we've screwed up the buffer, resetting\n");
	  inbuf->rbp = inbuf->buf;
	  inbuf->wbp = inbuf->buf;
	  return;
	}
      } else {
	//
	// Good command
	//
	ert[nert].sock    = pfd->fd;
	ert[nert].peer    = fromaddr;
	ert[nert].bufsize = 0;
	ert[nert].buf     = NULL;
	cmds[cmd](inbuf, (ert+nert));

	if( ert[nert].bufsize >= 0) {
	  // save responses for the end
	  if( nert >= (sizeof( ert) / sizeof( ert[0]))) {
	    fprintf( stderr, "main: too many response buffers needed.  Max is %lu\n", sizeof( ert)/sizeof( ert[0]));
	    //
	    // Really, this should not happen in real life.
	    // It looks like we'll drop some packets if it does happen.
	    //
	    break;
	  }
	  nert++;
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
      
	mk_reply( inbuf, rsize, reply, &fromaddr, sizeof( fromaddr));
      }

    }
  }
  //  printf( "\n");
}


/** Virtual circuit listener server
 *
 * \param pfd   pollfd object for this socket
 * \param inbuf Our data buffer
 */
void vclistener_service( struct pollfd *pfd) {
  static struct sockaddr_in fromaddr;		// client's address
  static int fromlen;				// used and ignored to store length of client address
  static e_socks_buffer_t *inbuf = NULL;	// our socket buffer
  int newsock;
  
  if( inbuf == NULL) {
    inbuf = find_e_socks_buffer( pfd->fd);
    if( inbuf == NULL) {
      fprintf( stderr, "vclistener_service: could not find buffer for socket %d\n", pfd->fd);
      return;
    }
  }

  fromlen = sizeof( fromaddr);
  newsock = accept( pfd->fd, (struct sockaddr *)&fromaddr, (unsigned int *)&fromlen);

  fprintf( stderr, "accepted socket %d from %s (vclistener_service)\n", newsock, inet_ntoa( fromaddr.sin_addr));

  if( newsock < 0) {
    return;
  }
  if( n_e_socks < n_e_socks_max) {
    e_socks_buf_init( newsock);
  } else {
    fprintf( stderr, "vclistener: too many sockets open (%d) to open another\n", n_e_socks);
  }
}



e_kvpair_t *related_kvpair( e_kvpair_t *parent, char *related) {
  char otherkey[128];
  ENTRY entry_in, *entry_outp;
  e_kvpair_t *rtn;

  if( strlen(parent->kvname) + strlen(related) + 2 > sizeof( otherkey))
    return NULL;

  sprintf( otherkey, "%s:%s", parent->kvname, related);
  entry_in.key = otherkey;
  entry_outp = hsearch( entry_in, FIND);
  if( entry_outp == NULL)
    return NULL;
  rtn = entry_outp->data;
  if( rtn == NULL)
    return NULL;
  
  rtn->parent = parent;
  return entry_outp->data;
}

void update_ht() {
  static int kv_key_col, kv_name_col, kv_value_col, kv_seq_col, kv_dbr_col, kv_epoch_secs_col, kv_epoch_nsecs_col, first_time=1;
  void *params[1];
  int param_lengths[1];
  int param_formats[1];
  PGresult *pgr;
  ENTRY entry_in, *entry_outp;
  uint32_t nseq, newseq;
  int i;
  e_kvpair_t *kvpp, *array_length_kvpp;
  e_array_t  *an_array;

  nseq = htonl( ht_seq);
  params[0] = &nseq;	param_lengths[0] = sizeof(nseq);	param_formats[0] = 1;
  pgr = e_execPrepared( "getkvs", 1, (const char **)params, param_lengths, param_formats, 1);

  if( pgr == NULL) {
    fprintf( stderr, "update_ht: null response from database server\n");
    return;
  }

  if( first_time) {
    kv_key_col         = PQfnumber( pgr, "kv_key");
    kv_name_col        = PQfnumber( pgr, "kv_name");
    kv_value_col       = PQfnumber( pgr, "kv_value");
    kv_seq_col         = PQfnumber( pgr, "kv_seq");
    kv_dbr_col         = PQfnumber( pgr, "kv_dbr");
    kv_epoch_secs_col  = PQfnumber( pgr, "kv_epoch_secs");
    kv_epoch_nsecs_col = PQfnumber( pgr, "kv_epoch_nsecs");
    first_time = 0;
  }

  for( i=0; i<PQntuples( pgr); i++) {

    newseq = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_seq_col));
    if( ht_seq < newseq)
      ht_seq = newseq;

    entry_in.key = PQgetvalue( pgr, i, kv_name_col);
    entry_outp = hsearch( entry_in, FIND);
    if( entry_outp != NULL && entry_outp->data != NULL) {
      //
      // Update this one
      //
      kvpp = entry_outp->data;
      //
      // Unless it has the same value
      //
      if( strcmp( kvpp->kvvalue, PQgetvalue( pgr, i, kv_value_col)) != 0) {

	if( kvpp->kvvalue != NULL)
	  free( kvpp->kvvalue);
	kvpp->kvvalue        = strdup( PQgetvalue( pgr, i, kv_value_col));
	kvpp->kvseq          = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_seq_col));
	kvpp->eepoch_secs    = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_epoch_secs_col));
	kvpp->eepoch_nsecs   = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_epoch_nsecs_col));

	notify_subscribers( kvpp);
      }
    } else {
      //
      // Add this one
      // Since we are adding this now there is no possiblity that we need to notify a channel subscriber
      //
      kvpp = calloc( 1, sizeof( e_kvpair_t));
      if( kvpp == NULL) {
	fprintf( stderr, "update_ht: out of memory\n");
	exit( 1);
      }
      kvpp->kvkey          = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_key_col));
      kvpp->kvname         = strdup( PQgetvalue( pgr, i, kv_name_col));
      kvpp->kvvalue        = strdup( PQgetvalue( pgr, i, kv_value_col));
      kvpp->dbr_type       = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_dbr_col));
      kvpp->kvseq          = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_seq_col));
      kvpp->eepoch_secs    = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_epoch_secs_col));
      kvpp->eepoch_nsecs   = ntohl( *(uint32_t *)PQgetvalue( pgr, i, kv_epoch_nsecs_col));
      kvpp->chans          = NULL;
      kvpp->array          = NULL;
      kvpp->array_length   = 0;

      //
      // Note that the pg query that generates these kvnames sorts it by decreasing order of kvname length
      // which means that the related pairs will be entered into the hash table before the main value
      // and therefore we do not also have to check the inverse of the related_kvpair
      //
      kvpp->high_limit     = related_kvpair( kvpp, "maxPosition");
      kvpp->low_limit      = related_kvpair( kvpp, "minPosition");
      kvpp->high_limit_hit = related_kvpair( kvpp, "posLimitSet");
      kvpp->low_limit_hit  = related_kvpair( kvpp, "negLimitSet");
      kvpp->prec           = related_kvpair( kvpp, "printPrecision");
      array_length_kvpp    = related_kvpair( kvpp, "length");
      if( array_length_kvpp != NULL) {
	int i;
	char *the_key;

	kvpp->array_length = strtol( array_length_kvpp->kvvalue, NULL, 10);
	the_key = calloc( 1, sizeof( kvpp->kvname) + 32);
	
	//
	// create in reverse order so at least initially we have them in normal order when done
	//
	for( i = kvpp->array_length - 1; i >= 0; i--) {
	  an_array = calloc( 1, sizeof( e_array_t));

	  an_array->index = i;
	  sprintf( the_key, "%s:%d:name", kvpp->kvname, i);
	  entry_in.key = the_key;
	  entry_outp = hsearch( entry_in, FIND);
	  if( entry_outp != NULL)
	    an_array->name  = entry_outp->data;

	  sprintf( the_key, "%s:%d:position", kvpp->kvname, i);
	  entry_in.key = the_key;
	  entry_outp = hsearch( entry_in, FIND);
	  if( entry_outp != NULL)
	    an_array->position  = entry_outp->data;

	  an_array->next = kvpp->array;
	  kvpp->array    = an_array;
	}
      }

      kvpp->sid            = ht_n;
      kvpp->next           = kvpair_list;
      kvpair_list          = kvpp;
      entry_in.key         = kvpp->kvname;
      entry_in.data        = kvpp;

      hsearch( entry_in, ENTER);	// we trust our ability to keep the hash table no more that half full
      
      kvpp->sid_key = calloc( 9, 1);
      sprintf( kvpp->sid_key, "%08x", ht_n);
      entry_in.key  = kvpp->sid_key;
      entry_in.data = kvpp;

      hsearch( entry_in, ENTER);	// we trust our ability to keep the hash table no more that half full

      ht_n++;
      if( ht_n * 4 > ht_max_size) {
	hdestroy();
	ht_max_size *= 2;

	hcreate( ht_max_size);
	for( kvpp= kvpair_list; kvpp != NULL; kvpp = kvpp->next) {
	  entry_in.key  = kvpp->kvname;
	  entry_in.data = kvpp;
	  hsearch( entry_in, ENTER);

	  entry_in.key  = kvpp->sid_key;
	  entry_in.data = kvpp;
	  hsearch( entry_in, ENTER);
	}
      }
    }
  }
}


/** Send out our broadcast beacon
 *  Set up as a signal handler for a timer
 *
 * \param sig  The timer signal (SIGALRM).
*/
void broadcast_beacon( int sig) {
  static int beaconid = 1;
  static struct itimerval timer_interval;
  int change_timer;
  e_response_t ert;
  uint32_t tmp;

  //
  // fix up the timer
  //
  change_timer = 0;
  getitimer( ITIMER_REAL, &timer_interval);
  if( timer_interval.it_interval.tv_sec == 0) {
    if( timer_interval.it_interval.tv_usec < 500000) {
      timer_interval.it_interval.tv_usec *= 2;
      change_timer = 1;
    } else {
      timer_interval.it_interval.tv_sec  = 1;
      timer_interval.it_interval.tv_usec = 0;
      change_timer = 1;
    }
  } else {
    if( timer_interval.it_interval.tv_sec < 8) {
      timer_interval.it_interval.tv_sec *= 2;
      change_timer = 1;
    }
  }
  
  if( change_timer) {
    timer_interval.it_value.tv_sec  =  timer_interval.it_interval.tv_sec;
    timer_interval.it_value.tv_usec = timer_interval.it_interval.tv_usec;
    if( setitimer( ITIMER_REAL, &timer_interval, NULL) == -1) {
      perror( "timer initialization");
    }
  }

  //
  // We've already converted the address to network byte order
  // now convert it back so we can convert it one more time.
  // Dizzy yet?
  //
  tmp = ntohl(ouraddr.sin_addr.s_addr);

  // Our outgoing packet
  //
  //           cmd: 13
  //  payload size:  0
  //   server port: for us it is 5064
  //   data length: 0
  //     Beacon ID: sequential number
  //    perhaps ip: 0 or our ip address
  //
  create_message( &ert, 13, 0, 5064, 0, beaconid++, tmp);
  mk_reply( e_sock_bufs + beacon_index, ert.bufsize, ert.buf, &broadcastaddr, sizeof( broadcastaddr));

}



/** our main routine (of course)
 */
int main( int argc, char **argv) {
  static int sock;			// our main socket
  static int vclistener;		// our tcp listener
  static struct sockaddr_in addr;	// our address
  static struct sigaction alarm_action; // sets up alarm signal function
  static struct itimerval timer_interval;
  static sigset_t emptyset, blockset;		// signal masks
  struct timespec update_timeout, now, last;
  int err;				// error return from bind
  int i;				// loop for poll response and sockets
  int nfds;				// number of active file descriptors from poll
  int flags;				// used to set non-blocking io for vclistener
  int opt_param;			// setsockot parameter
  int run_update_ht;			// flag indicating we have un serviced notifies pending on the DB


  fprintf( stderr, "initialization starting\n");

  // flag all the socket buffers to be available
  //
  for( i=0; i < n_e_socks_max; i++) {
    e_socks[i].fd          = -1;
    e_sock_bufs[i].sock    = -1;
    e_sock_bufs[i].buf     = NULL;
    e_sock_bufs[i].bufsize = 0;
  }

  //
  // pgres
  //
  pg_conn();

  //
  // Create and initialize our hash table
  //
  hcreate( ht_max_size);
  update_ht();
  run_update_ht = 0;

  //
  // UDP comms
  //
  sock = socket( PF_INET, SOCK_DGRAM, 0);
  if( sock == -1) {
    fprintf( stderr, "Could not create udp socket\n");
    exit( -1);
  }
  
  inet_aton( "10.1.255.255", &(ournetmask_addr));

  addr.sin_family = AF_INET;
  addr.sin_port   = htons(5064);
  addr.sin_addr.s_addr = INADDR_ANY;
  //inet_aton( E_LISTEN_IP, &(addr.sin_addr));

  opt_param = 1;
  setsockopt ( sock, SOL_SOCKET, SO_REUSEADDR, &opt_param, sizeof( opt_param));
  opt_param = 1;
  setsockopt ( sock, SOL_SOCKET, SO_BROADCAST, &opt_param, sizeof( opt_param));

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
  //  addr.sin_addr.s_addr = INADDR_ANY;
  inet_aton( E_LISTEN_IP, &(addr.sin_addr));

  opt_param = 1;
  if( setsockopt ( beacons, SOL_SOCKET, SO_REUSEADDR, &opt_param, sizeof( opt_param)) == -1) {
    perror( "beacons REUSEADDR");
  }
  opt_param = 1;
  if( setsockopt ( beacons, SOL_SOCKET, SO_BROADCAST, &opt_param, sizeof( opt_param)) == -1) {
    perror( "beacons BROADCAST");
  }

  err = bind( beacons, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
  if( err == -1) {
    fprintf( stderr, "Cound not bind beacon socket\n");
    exit( -1);
  }

  broadcastaddr.sin_family = AF_INET;
  broadcastaddr.sin_port   = htons( 5065);
  broadcastaddr.sin_addr.s_addr = htonl(INADDR_BROADCAST);

  ouraddr.sin_family = AF_INET;
  ouraddr.sin_port   = htons(5064);
  inet_aton( E_LISTEN_IP, &(ouraddr.sin_addr));

  beacon_index = e_socks_buf_init( beacons);

  //
  // TCP Virtual Circuits
  //
  vclistener = socket( PF_INET, SOCK_STREAM, 0);
  fprintf( stderr, "vclistener socket: %d\n", vclistener);

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
  inet_aton( E_LISTEN_IP, &(addr.sin_addr));

  opt_param = 1;
  setsockopt ( vclistener, SOL_SOCKET, SO_REUSEADDR, &opt_param, sizeof( opt_param));

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

  //
  // block sigalrm
  //
  sigemptyset( &blockset);
  sigaddset( &blockset, SIGALRM);
  sigprocmask( SIG_BLOCK, &blockset, NULL);

  //
  // broadcast the beacon when the alarm comes in
  //
  alarm_action.sa_handler = broadcast_beacon;
  sigemptyset( &alarm_action.sa_mask);
  alarm_action.sa_flags   = 0;
  sigaction( SIGALRM, &alarm_action, NULL);

  //
  // set up periodic alarms
  //
  timer_interval.it_interval.tv_sec  = 0;
  timer_interval.it_interval.tv_usec = 200000;
  timer_interval.it_value.tv_sec     = 0;
  timer_interval.it_value.tv_usec    = 500000;

  if( setitimer( ITIMER_REAL, &timer_interval, NULL) == -1) {
    perror( "timer initialization");
  }

  clock_gettime( CLOCK_REALTIME, &last);

  fprintf( stderr, "initialization complete\n");
  while( 1) {

    //
    // Fill up the fd array
    // Ignore stdin, stdout, stderr
    for( i=0, sock=3; sock<=e_max_socket_number; sock++) {
      e_socks_buffer_t *sbuff;

      sbuff = find_e_socks_buffer( sock);
      if( sbuff == NULL)
	continue;

      e_socks[i].fd = sock;
      e_socks[i].events = POLLIN;

      if( sbuff->reply_q != NULL) {
	e_socks[i].events |= POLLOUT;
      }
      i++;
    }
    n_e_socks = i;

    update_timeout.tv_sec  = 0;
    update_timeout.tv_nsec = 200000000;
    //
    // unblock alarm signal and wait for file descriptors
    //
    sigemptyset( &emptyset);
    nfds = ppoll( e_socks, n_e_socks, &update_timeout, &emptyset);

    //
    // Check for active descriptors
    //
    for( i=0; nfds>0 && i<n_e_socks_max; i++) {
      if( e_socks[i].revents) {
	nfds--;
	
	if( e_socks[i].fd == vclistener) {
	  vclistener_service( e_socks+i);
	} else if( e_socks[i].fd == PQsocket(q)) {
	  //
	  // The only thing that would come over the pg socket
	  // would be a notify about a monitor update.
	  //
	  PQconsumeInput( q);
	  while( PQnotifies( q) != NULL);

	  //
	  // Update from postgresql, but not too often.
	  // This is to keep unruly KV pairs from beating up the database server too badly
	  //
	  clock_gettime( CLOCK_REALTIME, &now);
	  if( (now.tv_sec - last.tv_sec + (now.tv_nsec - last.tv_nsec)/1.e9) >= 0.2) {
	    update_ht();
	    last.tv_sec  = now.tv_sec;
	    last.tv_nsec = now.tv_nsec;
	    run_update_ht = 0;
	  } else {
	    run_update_ht = 1;
	  }
	} else {
	  ca_service( e_socks+i);
	}
      }
    }

    //
    // This is for when a notify came but we had just serviced the database.
    // We get to it eventually but keep from putting too big a load on the server.
    //
    if( run_update_ht) {
      clock_gettime( CLOCK_REALTIME, &now);
      if( (now.tv_sec - last.tv_sec + (now.tv_nsec - last.tv_nsec)/1.e9) >= 0.2) {
	update_ht();
	last.tv_sec  = now.tv_sec;
	last.tv_nsec = now.tv_nsec;
	run_update_ht = 0;
      }
    }
  }
  return 0;
}

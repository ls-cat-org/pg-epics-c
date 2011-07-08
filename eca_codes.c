#include <stdio.h>
#define CA_K_INFO       3   /* successful */
#define CA_K_ERROR      2   /* failed- continue */
#define CA_K_SUCCESS    1   /* successful */
#define CA_K_WARNING    0   /* unsuccessful */
#define CA_K_SEVERE     4   /* failed- quit */
#define CA_K_FATAL  CA_K_ERROR | CA_K_SEVERE

#define CA_M_MSG_NO     0x0000FFF8
#define CA_M_SEVERITY   0x00000007
#define CA_M_LEVEL      0x00000003
#define CA_M_SUCCESS    0x00000001
#define CA_M_ERROR      0x00000002
#define CA_M_SEVERE     0x00000004

#define CA_S_MSG_NO     0x0D
#define CA_S_SEVERITY   0x03

#define CA_V_MSG_NO     0x03
#define CA_V_SEVERITY   0x00
#define CA_V_SUCCESS    0x00

/* Define MACROS to extract/insert individual fields from a status value */

#define CA_EXTRACT_MSG_NO(code)\
  ( ( (code) & CA_M_MSG_NO )  >> CA_V_MSG_NO )
#define CA_EXTRACT_SEVERITY(code)\
  ( ( (code) & CA_M_SEVERITY )    >> CA_V_SEVERITY )
#define CA_EXTRACT_SUCCESS(code)\
  ( ( (code) & CA_M_SUCCESS )     >> CA_V_SUCCESS )

#define CA_INSERT_MSG_NO(code)\
  (   ((code)<< CA_V_MSG_NO)  & CA_M_MSG_NO   )
#define CA_INSERT_SEVERITY(code)\
  (   ((code)<< CA_V_SEVERITY)& CA_M_SEVERITY )
#define CA_INSERT_SUCCESS(code)\
  (   ((code)<< CA_V_SUCCESS) & CA_M_SUCCESS  )

#define DEFMSG(SEVERITY,NUMBER)\
  (CA_INSERT_MSG_NO(NUMBER) | CA_INSERT_SEVERITY(SEVERITY))
#define ECA_NORMAL          DEFMSG(CA_K_SUCCESS,    0) /* success */
#define ECA_MAXIOC          DEFMSG(CA_K_ERROR,      1) /* defunct */
#define ECA_UKNHOST         DEFMSG(CA_K_ERROR,      2) /* defunct */
#define ECA_UKNSERV         DEFMSG(CA_K_ERROR,      3) /* defunct */
#define ECA_SOCK            DEFMSG(CA_K_ERROR,      4) /* defunct */
#define ECA_CONN            DEFMSG(CA_K_WARNING,    5) /* defunct */
#define ECA_ALLOCMEM        DEFMSG(CA_K_WARNING,    6)
#define ECA_UKNCHAN         DEFMSG(CA_K_WARNING,    7) /* defunct */
#define ECA_UKNFIELD        DEFMSG(CA_K_WARNING,    8) /* defunct */
#define ECA_TOLARGE         DEFMSG(CA_K_WARNING,    9)
#define ECA_TIMEOUT         DEFMSG(CA_K_WARNING,   10)
#define ECA_NOSUPPORT       DEFMSG(CA_K_WARNING,   11) /* defunct */
#define ECA_STRTOBIG        DEFMSG(CA_K_WARNING,   12) /* defunct */
#define ECA_DISCONNCHID     DEFMSG(CA_K_ERROR,     13) /* defunct */
#define ECA_BADTYPE         DEFMSG(CA_K_ERROR,     14)
#define ECA_CHIDNOTFND      DEFMSG(CA_K_INFO,      15) /* defunct */
#define ECA_CHIDRETRY       DEFMSG(CA_K_INFO,      16) /* defunct */
#define ECA_INTERNAL        DEFMSG(CA_K_FATAL,     17)
#define ECA_DBLCLFAIL       DEFMSG(CA_K_WARNING,   18) /* defunct */
#define ECA_GETFAIL         DEFMSG(CA_K_WARNING,   19)
#define ECA_PUTFAIL         DEFMSG(CA_K_WARNING,   20)
#define ECA_ADDFAIL         DEFMSG(CA_K_WARNING,   21) /* defunct */
#define ECA_BADCOUNT        DEFMSG(CA_K_WARNING,   22)
#define ECA_BADSTR          DEFMSG(CA_K_ERROR,     23)
#define ECA_DISCONN         DEFMSG(CA_K_WARNING,   24)
#define ECA_DBLCHNL         DEFMSG(CA_K_WARNING,   25)
#define ECA_EVDISALLOW      DEFMSG(CA_K_ERROR,     26)
#define ECA_BUILDGET        DEFMSG(CA_K_WARNING,   27) /* defunct */
#define ECA_NEEDSFP         DEFMSG(CA_K_WARNING,   28) /* defunct */
#define ECA_OVEVFAIL        DEFMSG(CA_K_WARNING,   29) /* defunct */
#define ECA_BADMONID        DEFMSG(CA_K_ERROR,     30)
#define ECA_NEWADDR         DEFMSG(CA_K_WARNING,   31) /* defunct */
#define ECA_NEWCONN         DEFMSG(CA_K_INFO,      32) /* defunct */
#define ECA_NOCACTX         DEFMSG(CA_K_WARNING,   33) /* defunct */
#define ECA_DEFUNCT         DEFMSG(CA_K_FATAL,     34) /* defunct */
#define ECA_EMPTYSTR        DEFMSG(CA_K_WARNING,   35) /* defunct */
#define ECA_NOREPEATER      DEFMSG(CA_K_WARNING,   36) /* defunct */
#define ECA_NOCHANMSG       DEFMSG(CA_K_WARNING,   37) /* defunct */
#define ECA_DLCKREST        DEFMSG(CA_K_WARNING,   38) /* defunct */
#define ECA_SERVBEHIND      DEFMSG(CA_K_WARNING,   39) /* defunct */
#define ECA_NOCAST          DEFMSG(CA_K_WARNING,   40) /* defunct */
#define ECA_BADMASK         DEFMSG(CA_K_ERROR,     41)
#define ECA_IODONE          DEFMSG(CA_K_INFO,      42)
#define ECA_IOINPROGRESS    DEFMSG(CA_K_INFO,      43)
#define ECA_BADSYNCGRP      DEFMSG(CA_K_ERROR,     44)
#define ECA_PUTCBINPROG     DEFMSG(CA_K_ERROR,     45)
#define ECA_NORDACCESS      DEFMSG(CA_K_WARNING,   46)
#define ECA_NOWTACCESS      DEFMSG(CA_K_WARNING,   47)
#define ECA_ANACHRONISM     DEFMSG(CA_K_ERROR,     48)
#define ECA_NOSEARCHADDR    DEFMSG(CA_K_WARNING,   49)
#define ECA_NOCONVERT       DEFMSG(CA_K_WARNING,   50)
#define ECA_BADCHID         DEFMSG(CA_K_ERROR,     51)
#define ECA_BADFUNCPTR      DEFMSG(CA_K_ERROR,     52)
#define ECA_ISATTACHED      DEFMSG(CA_K_WARNING,   53)
#define ECA_UNAVAILINSERV   DEFMSG(CA_K_WARNING,   54)
#define ECA_CHANDESTROY     DEFMSG(CA_K_WARNING,   55)
#define ECA_BADPRIORITY     DEFMSG(CA_K_ERROR,     56)
#define ECA_NOTTHREADED     DEFMSG(CA_K_ERROR,     57)
#define ECA_16KARRAYCLIENT  DEFMSG(CA_K_WARNING,   58)
#define ECA_CONNSEQTMO      DEFMSG(CA_K_WARNING,   59)
#define ECA_UNRESPTMO       DEFMSG(CA_K_WARNING,   60)

int main() {
  printf( "ECA_NORMAL  %d\n",ECA_NORMAL);//        DEFMSG(CA_K_SUCCESS,    0) /* success */
  printf( "ECA_MAXIOC  %d  DEFUNCT\n",ECA_MAXIOC);//        DEFMSG(CA_K_ERROR,      1) /* defunct */
  printf( "ECA_UKNHOST  %d  DEFUNCT\n",ECA_UKNHOST);//       DEFMSG(CA_K_ERROR,      2) /* defunct */
  printf( "ECA_UKNSERV  %d  DEFUNCT\n",ECA_UKNSERV);//       DEFMSG(CA_K_ERROR,      3) /* defunct */
  printf( "ECA_SOCK  %d  DEFUNCT\n",ECA_SOCK);//          DEFMSG(CA_K_ERROR,      4) /* defunct */
  printf( "ECA_CONN  %d  DEFUNCT\n",ECA_CONN);//          DEFMSG(CA_K_WARNING,    5) /* defunct */
  printf( "ECA_ALLOCMEM  %d\n",ECA_ALLOCMEM);//      DEFMSG(CA_K_WARNING,    6)
  printf( "ECA_UKNCHAN  %d  DEFUNCT\n",ECA_UKNCHAN);//       DEFMSG(CA_K_WARNING,    7) /* defunct */
  printf( "ECA_UKNFIELD  %d  DEFUNCT\n",ECA_UKNFIELD);//      DEFMSG(CA_K_WARNING,    8) /* defunct */
  printf( "ECA_TOLARGE  %d\n",ECA_TOLARGE);//       DEFMSG(CA_K_WARNING,    9)
  printf( "ECA_TIMEOUT  %d\n",ECA_TIMEOUT);//       DEFMSG(CA_K_WARNING,   10)
  printf( "ECA_NOSUPPORT  %d  DEFUNCT\n",ECA_NOSUPPORT);//     DEFMSG(CA_K_WARNING,   11) /* defunct */
  printf( "ECA_STRTOBIG  %d  DEFUNCT\n",ECA_STRTOBIG);//      DEFMSG(CA_K_WARNING,   12) /* defunct */
  printf( "ECA_DISCONNCHID  %d  DEFUNCT\n",ECA_DISCONNCHID);//   DEFMSG(CA_K_ERROR,     13) /* defunct */
  printf( "ECA_BADTYPE  %d\n",ECA_BADTYPE);//       DEFMSG(CA_K_ERROR,     14)
  printf( "ECA_CHIDNOTFND  %d  DEFUNCT\n",ECA_CHIDNOTFND);//    DEFMSG(CA_K_INFO,      15) /* defunct */
  printf( "ECA_CHIDRETRY  %d  DEFUNCT\n",ECA_CHIDRETRY);//     DEFMSG(CA_K_INFO,      16) /* defunct */
  printf( "ECA_INTERNAL  %d\n",ECA_INTERNAL);//      DEFMSG(CA_K_FATAL,     17)
  printf( "ECA_DBLCLFAIL  %d  DEFUNCT\n",ECA_DBLCLFAIL);//     DEFMSG(CA_K_WARNING,   18) /* defunct */
  printf( "ECA_GETFAIL  %d\n",ECA_GETFAIL);//       DEFMSG(CA_K_WARNING,   19)
  printf( "ECA_PUTFAIL  %d\n",ECA_PUTFAIL);//       DEFMSG(CA_K_WARNING,   20)
  printf( "ECA_ADDFAIL  %d  DEFUNCT\n",ECA_ADDFAIL);//       DEFMSG(CA_K_WARNING,   21) /* defunct */
  printf( "ECA_BADCOUNT  %d\n",ECA_BADCOUNT);//      DEFMSG(CA_K_WARNING,   22)
  printf( "ECA_BADSTR  %d\n",ECA_BADSTR);//        DEFMSG(CA_K_ERROR,     23)
  printf( "ECA_DISCONN  %d\n",ECA_DISCONN);//       DEFMSG(CA_K_WARNING,   24)
  printf( "ECA_DBLCHNL  %d\n",ECA_DBLCHNL);//       DEFMSG(CA_K_WARNING,   25)
  printf( "ECA_EVDISALLOW  %d\n",ECA_EVDISALLOW);//    DEFMSG(CA_K_ERROR,     26)
  printf( "ECA_BUILDGET  %d  DEFUNCT\n",ECA_BUILDGET);//      DEFMSG(CA_K_WARNING,   27) /* defunct */
  printf( "ECA_NEEDSFP  %d  DEFUNCT\n",ECA_NEEDSFP);//       DEFMSG(CA_K_WARNING,   28) /* defunct */
  printf( "ECA_OVEVFAIL  %d  DEFUNCT\n",ECA_OVEVFAIL);//      DEFMSG(CA_K_WARNING,   29) /* defunct */
  printf( "ECA_BADMONID  %d\n",ECA_BADMONID);//      DEFMSG(CA_K_ERROR,     30)
  printf( "ECA_NEWADDR  %d  DEFUNCT\n",ECA_NEWADDR);//       DEFMSG(CA_K_WARNING,   31) /* defunct */
  printf( "ECA_NEWCONN  %d  DEFUNCT\n",ECA_NEWCONN);//       DEFMSG(CA_K_INFO,      32) /* defunct */
  printf( "ECA_NOCACTX  %d  DEFUNCT\n",ECA_NOCACTX);//       DEFMSG(CA_K_WARNING,   33) /* defunct */
  printf( "ECA_DEFUNCT  %d  DEFUNCT\n",ECA_DEFUNCT);//       DEFMSG(CA_K_FATAL,     34) /* defunct */
  printf( "ECA_EMPTYSTR  %d  DEFUNCT\n",ECA_EMPTYSTR);//      DEFMSG(CA_K_WARNING,   35) /* defunct */
  printf( "ECA_NOREPEATER  %d  DEFUNCT\n",ECA_NOREPEATER);//    DEFMSG(CA_K_WARNING,   36) /* defunct */
  printf( "ECA_NOCHANMSG  %d  DEFUNCT\n",ECA_NOCHANMSG);//     DEFMSG(CA_K_WARNING,   37) /* defunct */
  printf( "ECA_DLCKREST  %d  DEFUNCT\n",ECA_DLCKREST);//      DEFMSG(CA_K_WARNING,   38) /* defunct */
  printf( "ECA_SERVBEHIND  %d  DEFUNCT\n",ECA_SERVBEHIND);//    DEFMSG(CA_K_WARNING,   39) /* defunct */
  printf( "ECA_NOCAST  %d  DEFUNCT\n",ECA_NOCAST);//        DEFMSG(CA_K_WARNING,   40) /* defunct */
  printf( "ECA_BADMASK  %d\n",ECA_BADMASK);//       DEFMSG(CA_K_ERROR,     41)
  printf( "ECA_IODONE  %d\n",ECA_IODONE);//        DEFMSG(CA_K_INFO,      42)
  printf( "ECA_IOINPROGRESS  %d\n",ECA_IOINPROGRESS);//  DEFMSG(CA_K_INFO,      43)
  printf( "ECA_BADSYNCGRP  %d\n",ECA_BADSYNCGRP);//    DEFMSG(CA_K_ERROR,     44)
  printf( "ECA_PUTCBINPROG  %d\n",ECA_PUTCBINPROG);//   DEFMSG(CA_K_ERROR,     45)
  printf( "ECA_NORDACCESS  %d\n",ECA_NORDACCESS);//    DEFMSG(CA_K_WARNING,   46)
  printf( "ECA_NOWTACCESS  %d\n",ECA_NOWTACCESS);//    DEFMSG(CA_K_WARNING,   47)
  printf( "ECA_ANACHRONISM  %d\n",ECA_ANACHRONISM);//   DEFMSG(CA_K_ERROR,     48)
  printf( "ECA_NOSEARCHADDR  %d\n",ECA_NOSEARCHADDR);//  DEFMSG(CA_K_WARNING,   49)
  printf( "ECA_NOCONVERT  %d\n",ECA_NOCONVERT);//     DEFMSG(CA_K_WARNING,   50)
  printf( "ECA_BADCHID  %d\n",ECA_BADCHID);//       DEFMSG(CA_K_ERROR,     51)
  printf( "ECA_BADFUNCPTR  %d\n",ECA_BADFUNCPTR);//    DEFMSG(CA_K_ERROR,     52)
  printf( "ECA_ISATTACHED  %d\n",ECA_ISATTACHED);//    DEFMSG(CA_K_WARNING,   53)
  printf( "ECA_UNAVAILINSERV  %d\n",ECA_UNAVAILINSERV);// DEFMSG(CA_K_WARNING,   54)
  printf( "ECA_CHANDESTROY  %d\n",ECA_CHANDESTROY);//   DEFMSG(CA_K_WARNING,   55)
  printf( "ECA_BADPRIORITY  %d\n",ECA_BADPRIORITY);//   DEFMSG(CA_K_ERROR,     56)
  printf( "ECA_NOTTHREADED  %d\n",ECA_NOTTHREADED);//   DEFMSG(CA_K_ERROR,     57)
  printf( "ECA_16KARRAYCLIENT  %d\n",ECA_16KARRAYCLIENT);//DEFMSG(CA_K_WARNING,   58)
  printf( "ECA_CONNSEQTMO  %d\n",ECA_CONNSEQTMO);//    DEFMSG(CA_K_WARNING,   59)
  printf( "ECA_UNRESPTMO  %d\n",ECA_UNRESPTMO);//     DEFMSG(CA_K_WARNING,   60)
}

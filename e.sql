DROP SCHEMA e CASCADE;
CREATE SCHEMA e;
GRANT USAGE ON SCHEMA e TO PUBLIC;


CREATE OR REPLACE FUNCTION e.init() returns void as $$
  DECLARE
  BEGIN
    DELETE FROM e.created_channels;
    DELETE FROM e.monitors;
    LISTEN EPICS_MONITOR_UPDATE;
    LISTEN REDIS_KV_CONNECTOR;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.init() OWNER TO lsadmin;

CREATE TABLE e.beacons (
       bkey serial primary key,
       bts timestamp with time zone default now(),
       bid int,
       bip inet not null unique
);
ALTER TABLE e.beacons OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.beacon_update( thebip inet, thebid int) returns void as $$
  DECLARE
    
  BEGIN
    IF thebip is null or thebip = '0.0.0.0'::inet THEN
      return;
    END IF;
    PERFORM 1 FROM e.beacons WHERE bip=thebip;
    IF FOUND THEN
      UPDATE e.beacons SET bts = now(), bid = thebid WHERE bip=thebip;
    ELSE
      INSERT INTO e.beacons (bid, bip) VALUES (thebid, thebip);
    END IF;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.beacon_update( inet, int) OWNER TO lsadmin;


CREATE TABLE e.channel_searches (
       cskey serial primary key,
       cstsfirst timestamp with time zone default now(),
       cstslast  timestamp with time zone default now(),
       cscount int default 1,
       csip inet,
       cspversion int,
       cschanname text
);
ALTER TABLE e.channel_searches OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.channel_search( theIp inet, thepversion int, theChan text) returns boolean as $$
  DECLARE
    theKey int;
    thekv int;
    theKvname text;
  BEGIN
    SELECT INTO theKey cskey  FROM e.channel_searches WHERE csip=theip and cspversion=thepversion and cschanname=theChan;
    IF FOUND THEN
      UPDATE e.channel_searches SET cscount=cscount+1, cstslast=now() WHERE cskey=theKey;
    ELSE
      INSERT INTO e.channel_searches (csip, cspversion, cschanname) VALUES (theip, thepversion, thechan);
    END IF;

    IF not (theip << '10.1.0.0/16'::inet) THEN
      -- for now ignore requests from other than 10.1.0.0/16
      return false;
    END IF;

    SELECT INTO thekv e.channel_name_to_kv( theChan);
    IF FOUND and thekv is not null THEN
      return TRUE;
    END IF;

    IF thechan like '%.DESC' and (theChan like '21:mung:%' or theChan like '21:orange:' or theChan like '21:mango:' or theChan like '21:kiwi') then
      SELECT INTO theKvname e.channel_name_to_kvname( theChan);
      return TRUE;
    END IF;
    return FALSE;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.channel_search( inet, int, text) OWNER TO lsadmin;

CREATE TABLE e.used_host_user_pairs(
       uhupkey serial primary key,
       uhupfirst timestamp with time zone default now(),
       uhuplast timestamp with time zone default now(),
       uhupcount int default 1,
       uhupip inet,
       uhuphost text,
       uhupuser text
);
ALTER TABLE e.used_host_user_pairs OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.used_host_user_pair_add( theIp inet, theHost text, theUser text) returns void as $$
  DECLARE
    theKey int;
  BEGIN
    SELECT INTO theKey uhupkey FROM e.used_host_user_pairs WHERE uhupip=theIp and uhuphost=theHost and uhupuser=theUser;
    IF FOUND THEN
      UPDATE e.used_host_user_pairs SET uhupcount = uhupcount+1, uhuplast=now() WHERE uhupkey=theKey;
    ELSE
      INSERT INTO e.used_host_user_pairs (uhupip, uhuphost, uhupuser) VALUES (theIp, theHost, theUser);
    END IF;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.used_host_user_pair_add( inet, text, text) OWNER TO lsadmin;


CREATE TABLE e.channel_name_to_station (
       cntskey serial primary key,
       cntsstn int not null,
       cntsname text not null
);
ALTER TABLE e.channel_name_to_station OWNER TO lsadmin;
INSERT INTO e.channel_name_to_station (cntsstn, cntsname) VALUES ( 1, '21:mung:');
INSERT INTO e.channel_name_to_station (cntsstn, cntsname) VALUES ( 2, '21:orange:');
INSERT INTO e.channel_name_to_station (cntsstn, cntsname) VALUES ( 3, '21:kiwi:');
INSERT INTO e.channel_name_to_station (cntsstn, cntsname) VALUES ( 4, '21:mango:');

CREATE OR REPLACE FUNCTION e.channel_name_to_kv( theChan text) returns int as $$
  DECLARE
    theStn int;
    theK   text;
    rtn    int;
  BEGIN
    SELECT INTO theStn,theK cntsstn,replace(replace( theChan,cntsname,''),':','.') FROM e.channel_name_to_station WHERE theChan like cntsname||'%' LIMIT 1;
    IF NOT FOUND THEN
      return null;
    END IF;
    SELECT INTO rtn kvkey FROM px.kvs WHERE kvname=thek or kvname='stns.'||thestn||'.'||thek or kvname||'$' = 'stns.'||thestn||'.'||thek or kvname||'.$' = 'stns.'||thestn||'.'||thek;
    IF NOT FOUND THEN
      return null;
    END IF;
    return rtn;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.channel_name_to_kv( text) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.channel_name_to_kvname( theChan text) returns text as $$
  DECLARE
    rtn    text;
  BEGIN
    SELECT INTO rtn 'stns.' || cntsstn || '.' || replace(replace( theChan,cntsname,''),':','.') FROM e.channel_name_to_station WHERE theChan like cntsname||'%' LIMIT 1;
    IF NOT FOUND THEN
      return null;
    END IF;
    return rtn;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.channel_name_to_kvname( text) OWNER TO lsadmin;

CREATE TABLE e.dbrs (
       dkey serial primary key,
       dtype int,       -- CA type
       dname text,      -- a comment for us to stay sane
       dplsize int,     -- overhead payload (size without the value)
       ddsize int       -- size of the type (if knowable)
);
ALTER TABLE e.dbrs OWNER to lsadmin;

CREATE TYPE e.get_dbr_payload_size_type as ( plsize int, dsize int);
CREATE OR REPLACE FUNCTION e.get_dbr_payload_size( dbr int) returns e.get_dbr_payload_size_type as $$
  SELECT dplsize, ddsize FROM e.dbrs WHERE dtype=$1;
$$ LANGUAGE SQL SECURITY DEFINER STABLE;
ALTER FUNCTION e.get_dbr_payload_size( int) OWNER TO lsadmin;


INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  0, 'string', 0, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  1, 'short',  0, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  2, 'float',  0, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  3, 'enum',   0, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  4, 'char',   0, 1);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  5, 'long',   0, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  6, 'double', 0, 8);

INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  7, 'sts_string', 4, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  8, 'sts_short',  4, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES (  9, 'sts_float',  4, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 10, 'sts_enum',   4, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 11, 'sts_char',   5, 1);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 12, 'sts_long',   4, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 13, 'sts_double', 8, 8);

INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 14, 'time_string', 12, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 15, 'time_short',  14, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 16, 'time_float',  12, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 17, 'time_enum',   14, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 18, 'time_char',   15, 1);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 19, 'time_long',   12, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 20, 'time_double', 16, 8);

INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 21, 'gr_string',   0, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 22, 'gr_short',   24, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 23, 'gr_float',   40, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 24, 'gr_enum',   422, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 25, 'gr_char',    19, 1);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 26, 'gr_long',    36, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 27, 'gr_double',  64, 8);

INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 28, 'crtl_string',  0, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 29, 'crtl_short',  28, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 30, 'crtl_float',  48, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 31, 'crtl_enum',  422, 2);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 32, 'crtl_char',   21, 1);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 33, 'crtl_long',   44, 4);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 34, 'crtl_double', 80, 8);

INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 35, 'unknown_35',   0, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 36, 'unknown_36',   0, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 37, 'stack_string', 0, 0);
INSERT INTO e.dbrs (dtype, dname, dplsize, ddsize) VALUES ( 38, 'class_name',   0, 0);

drop type e.get_values_type cascade;
CREATE TYPE e.get_values_type AS ( val text, eepoch int, ensec int, high_limit text, low_limit text, high_limit_hit int, low_limit_hit int, prec int);
CREATE OR REPLACE FUNCTION e.get_values( sid int) returns setof e.get_values_type as $$
  DECLARE
    rtn e.get_values_type;
    theepoch numeric;
  BEGIN
    FOR rtn.val, theepoch
        IN SELECT
        kvvalue, extract( epoch from (kvts - '1990-01-01 00:00:00-00'::timestamptz))
      FROM px.kvs
      LEFT JOIN e.created_channels on cckv=kvkey
      WHERE ccsid=sid
      LOOP

      rtn.eepoch := (floor(theepoch))::int;
      rtn.ensec  := (floor((theepoch - rtn.eepoch) * 1000000000))::int;
      SELECT INTO rtn.high_limit     coalesce( kvvalue, '0') FROM e.created_channels LEFT JOIN px.kvs ON cchighlimitkv=kvkey WHERE ccsid=sid;
      SELECT INTO rtn.low_limit      coalesce( kvvalue, '0') FROM e.created_channels LEFT JOIN px.kvs ON cclowlimitkv=kvkey  WHERE ccsid=sid;
      SELECT INTO rtn.high_limit_hit (coalesce( kvvalue, '0'))::int FROM e.created_channels LEFT JOIN px.kvs ON cchighlimithitkv=kvkey WHERE ccsid=sid;
      SELECT INTO rtn.low_limit_hit  (coalesce( kvvalue, '0'))::int FROM e.created_channels LEFT JOIN px.kvs ON cclowlimithitkv=kvkey  WHERE ccsid=sid;
      SELECT INTO rtn.prec           (coalesce( kvvalue, '0'))::int FROM e.created_channels LEFT JOIN px.kvs ON ccpreckv=kvkey      WHERE ccsid=sid;

      return next rtn;
    END LOOP;
    return;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.get_values( int) OWNER TO lsadmin;


CREATE OR REPLACE FUNCTION e.set_str_value( sid int, thevalue text) returns int as $$
  DECLARE
    thekvkey int;
    thekvname text;
    md2String text;
    theCmd text;
    theStn int;
  BEGIN
    SELECT INTO thekvkey, theCmd, thestn  cckv, kvmd2cmd, kvstn FROM e.created_channels left join px.kvs on cckv=kvkey WHERE ccsid=sid;
    IF FOUND THEN
      IF theCmd is not NULL and thestn is not null THEN
        md2String := theCmd || ' ' || thevalue;
        PERFORM px.md2pushqueue( theStn, md2String);
        RETURN 1;
      ELSE
        UPDATE px.kvs SET kvts=now(), kvvalue = thevalue, kvseq=nextval( 'px.kvs_kvseq_seq') WHERE kvkey=thekvkey;
	NOTIFY EPICS_MONITOR_UPDATE;
        RETURN 1;	-- success return code (ECA_NORMAL)
      END IF;
    END IF;
    RETURN 160;	-- success return code (ECA_PUTFAIL)
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.set_str_value( int, text) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.set_value( thekvkey int, thevalue text) returns int as $$
  DECLARE
    thekvname text;
    md2String text;
    theCmd text;
    theStn int;
  BEGIN
    SELECT INTO theCmd, thestn  kvmd2cmd, kvstn FROM px.kvs WHERE kvkey=thekvkey;
    IF FOUND THEN
      IF theCmd is not NULL and thestn is not null THEN
        md2String := theCmd || ' ' || thevalue;
        PERFORM px.md2pushqueue( theStn, md2String);
        RETURN 1;
      ELSE
        UPDATE px.kvs SET kvts=now(), kvvalue = thevalue, kvseq=nextval( 'px.kvs_kvseq_seq') WHERE kvkey=thekvkey;
	NOTIFY EPICS_MONITOR_UPDATE;
        RETURN 1;	-- success return code (ECA_NORMAL)
      END IF;
    END IF;
    RETURN 160;	-- success return code (ECA_PUTFAIL)
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.set_value( int, text) OWNER TO lsadmin;

CREATE TABLE e.created_channels (
       cckey serial primary key,
       ccts timestamp with time zone not null default now(),
       ccip inet   not null,
       cchost text not null,
       ccuser text not null,
       cccid int   not null,
       ccpversion int not null,
       cckv int not null,
       cchighlimitkv int default null,
       cclowlimitkv int default null,
       cchighlimithitkv int default null,
       cclowlimithitkv int default null,
       ccsid int unique
);
ALTER TABLE e.created_channels OWNER TO lsadmin;
CREATE INDEX cc_kv_index on e.created_channels (cckv);
CREATE INDEX cc_sid_index on e.created_channels (ccsid);

CREATE TYPE e.create_channel_type as ( sid int, dbr_type int, dcount int);
CREATE OR REPLACE FUNCTION e.create_channel(theip inet, thehost text, theuser text, thecid int, thepversion int, thechan text) returns e.create_channel_type as $$
  DECLARE
    thesid int;
    thekv  int;
    thehighlimitkv  int;
    thelowlimitkv  int;
    thehighlimithitkv  int;
    thelowlimithitkv  int;
    thepreckv int;
    forcechara boolean;
    channame text;
    rtn    e.create_channel_type;
  BEGIN
    LOOP
      -- get a unique sid
      thesid := floor(random() * 2^31);
      PERFORM 1 FROM e.created_channels WHERE ccsid=thesid;
      IF NOT FOUND and thesid != -1 THEN
        EXIT;
      END IF;
    END LOOP;


    --    IF not (theip << '10.1.0.0/16'::inet) THEN
    --      -- for now ignore requests from other than 10.1.0.0/16
    --      return NULL;
    --    END IF;

    IF position( '$' in thechan) = length(thechan) THEN
      forcechara := True;
      channame := substring( thechan for length(thechan)-1);
    ELSE
      channame := thechan;
    END IF;


    SELECT INTO thekv e.channel_name_to_kv( channame);
    IF NOT FOUND OR thekv is null THEN
      IF channame LIKE '%.DESC' THEN
        thekv := 8886;		-- big kludge
      END IF;
      IF thekv is null THEN
        RAISE NOTICE 'e.create_channel: failed request for channel "%"', thechan;
        return NULL;
      END IF;
    END IF;

    SELECT INTO thehighlimitkv    e.channel_name_to_kv( channame || '.maxPosition');
    SELECT INTO thelowlimitkv     e.channel_name_to_kv( channame || '.minPosition');
    SELECT INTO thehighlimithitkv e.channel_name_to_kv( channame || '.posLimitSet');
    SELECT INTO thelowlimithitkv  e.channel_name_to_kv( channame || '.negLimitSet');
    SELECT INTO thepreckv         e.channel_name_to_kv( channame || '.printPrecision');

    INSERT INTO e.created_channels (ccip,  cchost,  ccuser,  cccid,  ccpversion,  cckv,  ccsid,  cchighlimitkv,  cclowlimitkv,  cchighlimithitkv,  cclowlimithitkv, ccpreckv) VALUES
                                   (theip, thehost, theuser, thecid, thepversion, thekv, thesid,thehighlimitkv, thelowlimitkv, thehighlimithitkv, thelowlimithitkv, thepreckv);

    IF forcechara THEN
      rtn.dbr_type := 4;
      rtn.dcount   := 256;
    ELSE
      SELECT INTO rtn.dbr_type kvdbrtype FROM px.kvs WHERE kvkey=thekv;
      rtn.dcount   = 1;   -- only support scalars for now
    END IF;
    rtn.sid      = thesid;
    return rtn;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.create_channel( inet, text, text, int, int, text) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.clear_channel( theip inet, thesid int, thecid int) returns void as $$
  DECLARE
  BEGIN
    DELETE FROM e.created_channels WHERE ccip=theip and ccsid=thesid and cccid=thecid;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.clear_channel( inet, int, int) OWNER TO lsadmin;

CREATE TABLE e.monitors (
       mkey   serial primary key,
       mcreatets timestamp with time zone not null default now(),	-- time monitor was created
       mlastts timestamp with time zone not null default now(),		-- tme value was last sent
       mcc    int not null		-- channel we are monitoring
                references e.created_channels (cckey) ON DELETE CASCADE ON UPDATE CASCADE,
       mpaused boolean default True,	-- flags updates as active (true) or disabled (false)
       msock  int,			-- the socket: means something only to the client
       msubid int,			-- clients id for this monitor
       mmask int,			-- monitor mask received
       mdtype int,			-- the epics dbr type
       mcount int,			-- count of objects requested
       mkvseq int not null default 0	-- last sent value of kvseq for this channel
);
ALTER TABLE e.monitors OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.create_monitor( sid int, subid int, mask int, cnt int, sock int, dtype int) RETURNS setof e.get_values_type AS $$
  DECLARE
  BEGIN
    INSERT INTO e.monitors (mcc,   msock, msubid, mmask, mcount, mdtype, mkvseq)
                     SELECT cckey, sock,  subid,  mask,  cnt,    dtype,  kvseq
                       FROM e.created_channels
                       LEFT JOIN px.kvs on cckv=kvkey
                       WHERE ccsid=sid;
    RETURN QUERY SELECT * FROM  e.get_values( sid);
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.create_monitor( int, int, int, int, int, int) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.cancel_monitor( sid int, subid int) returns void as $$
  DECLARE
  BEGIN
    DELETE FROM e.monitors USING e.created_channels WHERE msubid=subid and ccsid=sid;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.cancel_monitor( int, int) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION e.remove_monitor( sock int) returns void as $$
--
-- Forcible removal of the monitor and the associated channel when a problem is
-- detected on a socket.
--
  DECLARE
    thecc int;
  BEGIN
    SELECT INTO thecc mcc FROM e.monitors WHERE msock=sock LIMIT 1;
    DELETE FROM e.monitors WHERE msock=sock;
    DELETE FROM e.created_channels WHERE cckey=thecc;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.remove_monitor( int) OWNER TO lsadmin;

CREATE TYPE e.check_monitor_type AS ( sid int, subid int, val text, sock int, dtype int, cnt int, eepoch int, ensec int);
CREATE OR REPLACE FUNCTION e.check_monitors() returns setof e.check_monitor_type AS $$
  DECLARE
    rtn e.check_monitor_type;
    newseq int;
    themkey int;
    theepoch numeric;
  BEGIN
    FOR           rtn.sid, rtn.subid, rtn.val, rtn.sock, rtn.dtype, rtn.cnt, theepoch,                               newseq, themkey
        IN SELECT ccsid,   msubid,    kvvalue, msock,    mdtype,    mcount,  extract( epoch from (kvts-'1990-1-1 00:00:00-00'::timestamptz)),  kvseq,   mkey
           FROM e.monitors
           LEFT JOIN e.created_channels ON mcc=cckey
           LEFT JOIN px.kvs ON cckv=kvkey
           WHERE kvseq > mkvseq LOOP
      UPDATE e.monitors set mlastts=now(), mkvseq=newseq WHERE mkey=themkey;
      rtn.eepoch := (floor(theepoch))::int;
      rtn.ensec  := (floor((theepoch - rtn.eepoch) * 1000000000))::int;
      return next rtn;
    END LOOP;
    return;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.check_monitors() OWNER TO lsadmin;



drop type e.getkvs_type cascade;
CREATE TYPE e.getkvs_type AS ( kv_key int, kv_name text, kv_value text, kv_seq int, kv_dbr int, kv_epoch_secs int, kv_epoch_nsecs int);
CREATE OR REPLACE FUNCTION e.getkvs( the_seq int) returns setof e.getkvs_type AS $$
  DECLARE
    rtn e.getkvs_type;
    theepoch numeric;
  BEGIN

    SELECT INTO rtn.kv_key, rtn.kv_name, rtn.kv_value, rtn.kv_seq, rtn.kv_dbr, theepoch
                kvkey,      kvname,      kvvalue,      kvseq,      kvdbrtype,      extract( epoch from (kvts - '1990-01-01 00:00:00'::timestamptz))
    FROM px.kvs WHERE kvname = 'epics.DESC';
    IF FOUND THEN
      rtn.kv_epoch_secs  := (floor(theepoch))::int;
      rtn.kv_epoch_nsecs := (floor((theepoch - rtn.kv_epoch_secs) * 1000000000))::int;
      return next rtn;
    END IF;

    FOR rtn.kv_key, rtn.kv_name, rtn.kv_value, rtn.kv_seq, rtn.kv_dbr, theepoch
       IN SELECT kvkey, replace(replace(kvname,'.',':'), 'stns:' || cntsstn || ':', cntsname) , kvvalue, kvseq, kvdbrtype, extract( epoch from (kvts - '1990-01-01 00:00:00'::timestamptz))
       FROM px.kvs
       LEFT JOIN e.channel_name_to_station on kvstn=cntsstn
       WHERE kvseq > the_seq and cntsstn is not null
       ORDER BY length(kvname) desc
       LOOP
      rtn.kv_epoch_secs  := (floor(theepoch))::int;
      rtn.kv_epoch_nsecs := (floor((theepoch - rtn.kv_epoch_secs) * 1000000000))::int;
      return next rtn;
    END LOOP;
    return;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION e.getkvs( int) OWNER TO lsadmin;

CREATE TABLE users (
       userid		serial PRIMARY KEY,
       username		varchar(30),
       password		varchar(255),
       email		varchar(30)
);

CREATE TABLE systems (
       sysid 	     serial PRIMARY KEY,
       sysname	     varchar(10),
       version	     varchar(30),
       githash	     varchar(255)
);

CREATE TABLE problems (
       probid	      serial PRIMARY KEY,
       sysid	      integer,
       probname	      varchar(30),
       githash	      varchar(255),
       FOREIGN KEY (sysid) REFERENCES systems	
);

CREATE TABLE locations (
       locid 	       serial PRIMARY KEY,
       location	       varchar(30),
       loginid	       varchar(10)
);

CREATE TABLE experiments (
       batchid 		 serial PRIMARY KEY,
       userid		 integer,
       probid		 integer,
       locid		 integer,
       batchdate	 date,
       FOREIGN KEY (userid) REFERENCES users,
       FOREIGN KEY (probid) REFERENCES problems,
       FOREIGN KEY (locid) REFERENCES locations
);

CREATE TABLE experiment (
       expid 		serial PRIMARY KEY,
       batchid		integer,
       param		varchar(60),
       value		varchar(255),
       FOREIGN KEY (batchid) REFERENCES experiments
);

CREATE TABLE generations (
       genid 		 integer,
       expid		 integer,
       param 		 varchar(60),
       value		 varchar(255),
       FOREIGN KEY (expid) REFERENCES experiment
);
set foreign_key_checks=0;
drop table if exists guser;
drop table if exists gfile;
drop table if exists edit;
set foreign_key_checks=1;

create table guser 
(
	username		varchar(30) not null,
	passwords		varchar(30) not null,
    primary key (username)
);

create table gfile
(
	id 				integer not null auto_increment,
    filename		varchar(30) not null,
    creator			varchar(30) not null,
    primary key (id),
    foreign key (creator) references guser(username) on delete cascade
);

create table edit
(
	id 				integer not null auto_increment,
    fileid			integer not null,
    editor			varchar(30) not null,
    edittime		time,
    primary key (id),
    foreign key (editor) references guser(username) on delete cascade,
    foreign key (fileid) references gfile(id) on delete cascade
);
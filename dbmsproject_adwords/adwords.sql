--create tables for result computed from 6 tasks--
create table adgreedy
(qid integer, rank integer, advertiserID integer, balance float, budget float);

create table adbalance
(qid integer, rank integer, advertiserID integer, balance float, budget float);

create table gebalance
(qid integer, rank integer, advertiserID integer, balance float, budget float);

create table secgreedy
(qid integer, rank integer, advertiserID integer, balance float, budget float);

create table secbalance
(qid integer, rank integer, advertiserID integer, balance float, budget float);

create table secgeblnc
(qid integer, rank integer, advertiserID integer, balance float, budget float);


--add table advertisers with frequence and balance--
alter table Advertisers
add (frequence integer default 0, balance float);

update Advertisers
set balance = budget;

--split query into tokens for matching keywords provided by advertisers--
create or replace type var_list as table of varchar(32767);
/

create or replace function split(in_value in varchar)
return var_list
is
	split_idx pls_integer;
	in_list varchar(500) := in_value;
	out_value var_list := var_list();
begin
	loop
	split_idx := instr(in_list, ' ');
	if split_idx > 0 then
		out_value.extend(1);
		out_value(out_value.last) := substr(in_list, 1, split_idx-1);
		in_list := substr(in_list, split_idx + length(' '));
	else
		out_value.extend(1);
		out_value(out_value.last) := in_list;
		return out_value;
	end if;
	end loop;
end split;
/

create table qtoken
(qid integer, keyword varchar(500));

declare
split_list var_list := var_list();
begin
for q_cur in (select qid, query from Queries) loop
	split_list := split(q_cur.query);
	for i in 1 .. split_list.count loop
		insert into qtoken values(q_cur.qid, split_list(i));
	end loop;
end loop;
end;
/

--compute the cosine similarity between eash matched pair of query and keyword--
--create a table including qid, advertiserID corresponding to the qid, cos-sim and the bid--
create table querymol as
(select qid, sqrt(sum(key_sqr)) as qmol from 
(select qid, count(keyword)*count(keyword) as key_sqr from qtoken group by qid, keyword)
group by qid);

create table adkeymol as
(select advertiserID, sqrt(sum(key_sqr)) as akmol from 
(select advertiserID, count(keyword)*count(keyword) as key_sqr from Keywords group by advertiserID, keyword)
group by advertiserID);

create table chosenkey as
(select qid, advertiserID, sum(key_frq) as sumAB, sum(bid) as bid from
(select qid, keyword, count(keyword) as key_frq from qtoken group by qid, keyword) natural join Keywords
group by qid, advertiserID);

create table allinfo as
(select qid, advertiserID, bid, sumAB/(qmol*akmol) as sim from chosenkey natural join
querymol natural join adkeymol);
--preprocess over--


create or replace procedure task1 (K in int, qnum in int)
is
rownum integer;
begin
for i in 1 .. qnum loop
	rownum := 0;
	for i_cur in (
	With A as (select qid, advertiserID, sim, bid from allinfo where qid = i)
	select qid, advertiserID, budget, balance, frequence, ctc, bid
	from A natural join Advertisers
	where balance > bid
	order by sim*ctc*bid desc)
	loop
		rownum := rownum + 1;
		if mod(i_cur.frequence, 100) < i_cur.ctc*100 then
			insert into adgreedy (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance-i_cur.bid, i_cur.budget);
			update Advertisers
			set balance = balance - i_cur.bid, frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		else
			insert into adgreedy (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance, i_cur.budget);
			update Advertisers
			set frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		end if;
	exit when rownum > K;
	end loop;
end loop;
update Advertisers
set balance = budget, frequence = 0;
end;
/

create or replace procedure task2 (K in int, qnum in int)
is
rownum integer;
begin
for i in 1 .. qnum loop
	rownum := 0;
	for i_cur in (
	With A as (select qid, advertiserID, sim, bid from allinfo where qid = i)
	select qid, advertiserID, budget, balance, frequence, ctc, bid
	from A natural join Advertisers
	where balance > bid
	order by sim*ctc*balance desc)
	loop
		rownum := rownum + 1;
		if mod(i_cur.frequence, 100) < i_cur.ctc*100 then
			insert into adbalance (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance-i_cur.bid, i_cur.budget);
			update Advertisers
			set balance = balance - i_cur.bid, frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		else
			insert into adbalance (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance, i_cur.budget);
			update Advertisers
			set frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		end if;
	exit when rownum > K;
	end loop;
end loop;
update Advertisers
set balance = budget, frequence = 0;
end;
/

create or replace procedure task3 (K in int, qnum in int)
is
rownum integer;
begin
for i in 1 .. qnum loop
	rownum := 0;
	for i_cur in (
	With A as (select qid, advertiserID, sim, bid from allinfo where qid = i)
	select qid, advertiserID, budget, balance, frequence, ctc, bid
	from A natural join Advertisers
	where balance > bid
	order by sim*ctc*bid*(1-exp(-balance/budget)) desc)
	loop
		rownum := rownum + 1;
		if mod(i_cur.frequence, 100) < i_cur.ctc*100 then
			insert into gebalance (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance-i_cur.bid, i_cur.budget);
			update Advertisers
			set balance = balance - i_cur.bid, frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		else
			insert into gebalance (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance, i_cur.budget);
			update Advertisers
			set frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		end if;
	exit when rownum > K;
	end loop;
end loop;
update Advertisers
set balance = budget, frequence = 0;
end;
/

create or replace procedure task4 (K in int, qnum in int)
is
rownum integer;
begin
for i in 1 .. qnum loop
	rownum := 0;
	for i_cur in (
	With A as (select * from allinfo natural join Advertisers where qid = i and balance > bid)
	select B.qid, B.advertiserID, B.balance, B.budget, B.frequence, B.ctc, case B.bid
	when (select min(bid) from A) then bid
	else (select max(bid) from A where bid < B.bid)
	end as newbid
	from A B
	order by B.sim*B.ctc*B.bid desc)
	loop
		rownum := rownum + 1;
		if mod(i_cur.frequence, 100) < i_cur.ctc*100 then
			insert into secgreedy (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance-i_cur.newbid, i_cur.budget);
			update Advertisers
			set balance = balance - i_cur.newbid, frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		else
			insert into secgreedy (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance, i_cur.budget);
			update Advertisers
			set frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		end if;
	exit when rownum > K;
	end loop;
end loop;
update Advertisers
set balance = budget, frequence = 0;
end;
/

create or replace procedure task5 (K in int, qnum in int)
is
rownum integer;
begin
for i in 1 .. qnum loop
	rownum := 0;
	for i_cur in (
	With A as (select * from allinfo natural join Advertisers where qid = i and balance > bid)
	select B.qid, B.advertiserID, B.balance, B.budget, B.frequence, B.ctc, case B.bid
	when (select min(bid) from A) then bid
	else (select max(bid) from A where bid < B.bid)
	end as newbid
	from A B
	order by B.sim*B.ctc*B.balance desc)
	loop
		rownum := rownum + 1;
		if mod(i_cur.frequence, 100) < i_cur.ctc*100 then
			insert into secbalance (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance-i_cur.newbid, i_cur.budget);
			update Advertisers
			set balance = balance - i_cur.newbid, frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		else
			insert into secbalance (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance, i_cur.budget);
			update Advertisers
			set frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		end if;
	exit when rownum > K;
	end loop;
end loop;
update Advertisers
set balance = budget, frequence = 0;
end;
/

create or replace procedure task6 (K in int, qnum in int)
is
rownum integer;
begin
for i in 1 .. qnum loop
	rownum := 0;
	for i_cur in (
	With A as (select * from allinfo natural join Advertisers where qid = i and balance > bid)
	select B.qid, B.advertiserID, B.balance, B.budget, B.frequence, B.ctc, case B.bid
	when (select min(bid) from A) then bid
	else (select max(bid) from A where bid < B.bid)
	end as newbid
	from A B
	order by B.sim*B.ctc*B.bid*(1-exp(-B.balance/B.budget)) desc)
	loop
		rownum := rownum + 1;
		if mod(i_cur.frequence, 100) < i_cur.ctc*100 then
			insert into secgeblnc (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance-i_cur.newbid, i_cur.budget);
			update Advertisers
			set balance = balance - i_cur.newbid, frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		else
			insert into secgeblnc (qid, rank, advertiserID, balance, budget)
			values (i_cur.qid, rownum, i_cur.advertiserID, i_cur.balance, i_cur.budget);
			update Advertisers
			set frequence = frequence + 1
			where advertiserID = i_cur.advertiserID;
		end if;
	exit when rownum > K;
	end loop;
end loop;
update Advertisers
set balance = budget, frequence = 0;
end;
/

exit

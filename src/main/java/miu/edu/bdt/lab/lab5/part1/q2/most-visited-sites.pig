-- most-visited-sites.pig
users = LOAD 'input/users.csv' using PigStorage(',') AS (user:chararray, age:int);
pages = LOAD 'input/pages.csv' using PigStorage(',') AS (user:chararray, page:chararray);
fltrd = FILTER users BY age >= 18 AND age <= 25;
joined = JOIN fltrd BY user, pages BY user;
grpd = GROUP joined BY page;
summed = FOREACH grpd GENERATE $0, COUNT($1);
sorted = ORDER summed BY $1 DESC;
limited = LIMIT sorted 5;
--DUMP limited;
STORE limited INTO 'output' USING PigStorage('\t');
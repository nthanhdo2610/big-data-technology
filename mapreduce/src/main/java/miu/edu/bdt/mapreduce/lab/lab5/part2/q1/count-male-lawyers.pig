--count-male-lawyers.pig
--How many male lawyers are listed in the users.txt file?
users = LOAD 'input/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
filtered = FILTER users BY gender =='M' AND occupation == 'lawyer';
grouped = GROUP filtered ALL;
count = FOREACH grouped GENERATE COUNT($1);
--DUMP count;
STORE count INTO 'output' USING PigStorage();
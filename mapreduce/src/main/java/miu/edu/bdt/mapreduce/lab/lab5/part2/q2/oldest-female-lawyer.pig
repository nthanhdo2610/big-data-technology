--oldest-female-lawyer.pig
--What is the userId of the oldest female lawyer?
users = LOAD 'input/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
filtered = FILTER users BY gender =='F' AND occupation == 'lawyer';
compacted = FOREACH filtered GENERATE $0, $1;
grouped = GROUP compacted BY $1;
sorted = ORDER grouped BY $0 DESC;
limited = LIMIT sorted 1;
flatBag = FOREACH limited GENERATE FLATTEN($1);
uniq = DISTINCT flatBag;
odest = FOREACH uniq GENERATE $0;
--DUMP limited;
STORE odest INTO 'output' USING PigStorage();

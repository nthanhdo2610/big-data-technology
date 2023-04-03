--pig -x local /home/cloudera/Desktop/JoinInPig.pig
rmf /home/cloudera/Desktop/Lab5_part1_Join_output

--Load and filter users
users = LOAD '/home/cloudera/Desktop/users.csv' USING PigStorage(',') AS (name: chararray, age: int);
usersFiltered = FILTER users BY age >= 18 AND age <= 25;

--Load pages
pages = LOAD '/home/cloudera/Desktop/pages.csv' USING PigStorage(',') AS (name: chararray, url: chararray);

--Join data
dataJoined = JOIN usersFiltered BY name, pages BY name;

--Group by url
groupedUrl = GROUP dataJoined BY url;

--Count the url clicks
clickCount =  FOREACH groupedUrl GENERATE group, COUNT(dataJoined) AS clicks;

--Order and Limit top 5
rankings = ORDER clickCount BY clicks DESC;
result = LIMIT rankings 5;

STORE result INTO '/home/cloudera/Desktop/Lab5_part1_Join_output' USING PigStorage(',');

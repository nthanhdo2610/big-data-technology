--Lab 5 - Part2 - Question 2: What is the userId of the oldest male lawyer? (write the complete pig script and the userId of the oldest male lawyer)
--Nhat Pham - 986847


--pig /home/cloudera/Desktop/Lab5_part2_question2.pig
rmf /home/cloudera/Desktop/Lab5_part2_question2_output;

users = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (userId: chararray, age: int, gender: chararray, occupation: chararray, zipCode: int);

maleLawyers = FILTER users BY occupation == 'lawyer' AND gender == 'M';
oldestAge = FOREACH (GROUP maleLawyers ALL) GENERATE MAX(maleLawyers.age) AS age;

oldestLawyers = FILTER maleLawyers BY age == oldestAge.age;

--Export the 1st person in case there are many male lawyer which has same age
top = LIMIT oldestLawyers 1;

result = FOREACH top GENERATE userId;

STORE result INTO '/home/cloudera/Desktop/Lab5_part2_question2_output';

--The userId of the oldest male lawyer: 10

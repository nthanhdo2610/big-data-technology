--Lab 5 - Part2 - Question 1: How many male lawyers are listed in the users.txt file? (write the complete pig script and the final count of male lawyers)
--Nhat Pham - 986847


--pig /home/cloudera/Desktop/Lab5_part2_question1.pig
rmf /home/cloudera/Desktop/Lab5_part2_question1_output;

users = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (userId: chararray, age: int, gender: chararray, occupation: chararray, zipCode: int);

maleLawyers = FILTER users BY occupation == 'lawyer' AND gender == 'M';
count = FOREACH (GROUP maleLawyers ALL) GENERATE COUNT(maleLawyers);

STORE count INTO '/home/cloudera/Desktop/Lab5_part2_question1_output';

/*The final count of male lawyers: 10
List of male lawyers
10	53	M	lawyer	90703
125	30	M	lawyer	22202
161	50	M	lawyer	55104
205	47	M	lawyer	6371
339	35	M	lawyer	37901
365	29	M	lawyer	20009
419	37	M	lawyer	43215
589	21	M	lawyer	90034
680	33	M	lawyer	90405
846	27	M	lawyer	47130
*/
userBag = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS
(userId:chararray,age:int,gender:chararray,occupation:chararray,zipCode:chararray);
filtered_male_lawyer = FILTER userBag BY gender == 'M' AND occupation == 'lawyer';
male_lawyer_group = GROUP filtered_male_lawyer ALL;
male_lawyer_count = FOREACH male_lawyer_group GENERATE COUNT($1);
STORE male_lawyer_count INTO 'output';

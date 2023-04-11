userBag = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS
(userId:chararray,age:int,gender:chararray,occupation:chararray,zipCode:chararray);
female_lawyers = FILTER userBag BY gender == 'F' AND occupation == 'lawyer';
f_lawyer_userId_age = FOREACH female_lawyers GENERATE userId, age;
f_lawyer_userId_age_group = GROUP f_lawyer_userId_age ALL;
oldest_female_lawyer_group = FOREACH f_lawyer_userId_age_group GENERATE MAX($1.age), $1.userId;
oldest_female_lawyer_userIds = FOREACH oldest_female_lawyer_group GENERATE flatten($1);
STORE oldest_female_lawyer_userIds INTO 'output';

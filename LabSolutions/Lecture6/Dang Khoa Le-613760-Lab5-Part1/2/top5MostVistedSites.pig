userBag = LOAD '/pig/users.csv' USING PigStorage(',') AS (user:chararray, age:int);
pageBag = LOAD '/pig/pages.csv' USING PigStorage(',') AS (user:chararray, page:chararray);
filteredUsers = FILTER userBag BY age >= 18 AND age <= 25;
joined_users_pages = JOIN filteredUsers BY user, pageBag BY user;
pageGroup = GROUP joined_users_pages BY page;
pageViewedCount = FOREACH pageGroup GENERATE group, COUNT(joined_users_pages);
topToBottomViewedPages = ORDER pageViewedCount BY $1 DESC;
top5MostViewedPages = LIMIT topToBottomViewedPages 5;
STORE top5MostViewedPages INTO 'output' USING PigStorage('*');

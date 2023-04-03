--Lab 5 - Part2 - Question 5: Out of these highest rated top 20 movies found in Q4, how many times male programmers have watched these movies?
--Nhat Pham - 986847


--pig /home/cloudera/Desktop/Lab5_part2_question5.pig
rmf /home/cloudera/Desktop/Lab5_part2_question5_output;

REGISTER '/usr/lib/pig/piggybank.jar';
DEFINE csvLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE csvStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

dataMovies = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING csvLoader() AS (movieId: long, title: chararray, genres: chararray);
dataRatings = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' USING PigStorage('\t') AS (userId: chararray, movieId: long, rating: int, timestamp: long);
dataUsers = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (userId: chararray, age: int, gender: chararray, occupation: chararray, zipCode: long);

--Gather genres and flatten to avoid multiple rows
moviesFlattened = FOREACH dataMovies GENERATE movieId, title, FLATTEN(STRSPLIT(genres, '\\|', 0)) AS genre;

--Filter movies which is 'Adventure'
moviesFiltered = FILTER moviesFlattened BY genre == 'Adventure';

--Clean movie genres
moviesAggred = FOREACH moviesFiltered GENERATE movieId AS movieId, 'Adventure' AS genre, title AS title;

--Filter movieId with top rating
movieId_topRatings = FILTER dataRatings BY rating == 5;

--Join movies & ratings
joinedData = JOIN moviesAggred BY movieId, movieId_topRatings BY movieId;

--Aggregate movies
moviesData = FOREACH joinedData GENERATE moviesAggred::movieId AS movieId, movieId_topRatings::rating AS rating, moviesAggred::title AS title;

--Sort to limit top 20 movies
sorted = ORDER (DISTINCT moviesData) BY rating DESC, movieId;

--Get the movieId at top 20 
top20 = LIMIT sorted 20;

--Get movie list with userId
moviesAndRatings = JOIN top20 BY movieId, movieId_topRatings BY movieId;
moviesList = FOREACH moviesAndRatings GENERATE top20::movieId AS movieId, top20::title AS title, movieId_topRatings::userId AS userId;

--Filter male programmers
maleProgrammers = FILTER dataUsers BY occupation == 'programmer' AND gender == 'M';

--Join moviesList and 
records = JOIN moviesList BY userId, maleProgrammers BY userId;

--Count
count = FOREACH (GROUP records ALL) GENERATE COUNT(records);

STORE count INTO '/home/cloudera/Desktop/Lab5_part2_question5_output/count' USING PigStorage('\t');

--Lab 5 - Part2 - Question 4:  Display a list of top 20 highest rated (rating=5) “Adventure” movies sorted by movieId.
--Nhat Pham - 986847


--pig /home/cloudera/Desktop/Lab5_part2_question4.pig
rmf /home/cloudera/Desktop/Lab5_part2_question4_output;

REGISTER '/usr/lib/pig/piggybank.jar';
DEFINE csvLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE csvStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

dataMovies = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING csvLoader() AS (movieId: long, title: chararray, genres: chararray);
dataRatings = LOAD '/home/cloudera/Desktop/MovieDataSet/rating.txt' USING PigStorage('\t') AS (userId: chararray, movieId: long, rating: int, timestamp: long);
--dataUsers = LOAD '/home/cloudera/Desktop/MovieDataSet/users.txt' USING PigStorage('|') AS (userId: chararray, age: int, gender: chararray, occupation: chararray, zipCode: long);

--Gather genres and flatten to avoid multiple rows
moviesFlattened = FOREACH dataMovies GENERATE movieId, title, FLATTEN(STRSPLIT(genres, '\\|', 0)) AS genre;

--Filter movies which is 'Adventure'
moviesFiltered = FILTER moviesFlattened BY genre == 'Adventure';

--Clean movie genres
moviesAggred = FOREACH moviesFiltered GENERATE movieId AS movieId, 'Adventure' AS genre, title AS title;

--Filter movieId with top rating
movieId_topRatings = FILTER dataRatings BY rating == 5;

--Join data
joinedData = JOIN moviesAggred BY movieId, movieId_topRatings BY movieId;

--Aggregate data in output format
dataOutput = FOREACH joinedData GENERATE moviesAggred::movieId AS movieId, moviesAggred::genre AS genre, movieId_topRatings::rating AS rating, moviesAggred::title AS title;

--Sort to limit top 20 movies
sorted = ORDER (DISTINCT dataOutput) BY rating DESC, movieId;

result = LIMIT sorted 20;

STORE result INTO '/home/cloudera/Desktop/Lab5_part2_question4_output' USING PigStorage('\t');

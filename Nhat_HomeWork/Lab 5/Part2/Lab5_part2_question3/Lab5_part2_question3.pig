--Lab 5 - Part2 - Question 3: How many movies are there whose title start with letter “A” or “a”? Show the count of these movies by genre.
--Nhat Pham - 986847


--pig /home/cloudera/Desktop/Lab5_part2_question3.pig
rmf /home/cloudera/Desktop/Lab5_part2_question3_output;

REGISTER '/usr/lib/pig/piggybank.jar';
DEFINE csvLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE csvStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

data = LOAD '/home/cloudera/Desktop/MovieDataSet/movies.csv' USING csvLoader() AS (movieId: chararray, title: chararray, genres: chararray);

--Gather genres and flatten to avoid multiple rows
flattened = FOREACH data GENERATE FLATTEN(STRSPLIT(genres, '\\|', 0)) as f;

--Group each flattened genre record
grouped = GROUP flattened BY f;

--Aggregate each genre record
result = FOREACH grouped GENERATE (chararray)group as genre, COUNT(flattened) as cnt;

--Sort each aggregate genre record
sorted = ORDER result BY genre;

STORE sorted INTO '/home/cloudera/Desktop/Lab5_part2_question3_output' USING csvStorage();

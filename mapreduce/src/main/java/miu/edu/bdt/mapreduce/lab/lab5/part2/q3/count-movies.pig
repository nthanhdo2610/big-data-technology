--count-movies.pig
-- How many movies are there whose title start with letter “A” or “a”? Show the count of these movies by genre.
--If a movie is both Action and Comedy then you’ll be
--counting it twice in both the Action and Comedy genres.
REGISTER hdfs://quickstart.cloudera:8020/user/cloudera/lib/piggybank-0.17.0.jar;
movies = LOAD 'input/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader AS (movieId:int, title:chararray, genres:chararray);
filtered = FILTER movies BY org.apache.pig.piggybank.evaluation.string.LOWER(org.apache.pig.piggybank.evaluation.string.SUBSTRING($1,0,1)) == 'a';
splited = FOREACH filtered GENERATE TOKENIZE($2,'|');
genres = FOREACH splited GENERATE FLATTEN($0);
grouped = GROUP genres BY $0;
counted = FOREACH grouped GENERATE $0, COUNT($1);
ordered = ORDER counted BY $0;
--DUMP limited;
STORE ordered INTO 'output' USING PigStorage('\t');

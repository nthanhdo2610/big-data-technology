--top-highest-rated.pig
--Display a list of top 20 highest rated (rating=5) “Adventure” movies sorted by movieId.
--movieId genres rating title
REGISTER hdfs://quickstart.cloudera:8020/user/cloudera/lib/piggybank-0.17.0.jar;
movies = LOAD 'input/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader AS (movieId:int, title:chararray, genres:chararray);
rating = LOAD 'input/rating.txt' USING PigStorage('\t') AS (userId:chararray, movieId:int, rating:int, timestamp:long);
ratingFiltered = FILTER rating BY $2 == 5;
splited = FOREACH movies GENERATE $0, $1, TOKENIZE($2,'|');
genres = FOREACH splited GENERATE $0, $1, FLATTEN($2);
movieFiltered = FILTER genres BY $2 == 'Adventure';
joined = JOIN movieFiltered BY $0, ratingFiltered BY $1;
compacted = FOREACH joined GENERATE $0, $2, $5, $1;
uniq = DISTINCT compacted;
sorted = ORDER uniq BY $0;
topHighest = LIMIT sorted 20;
--DUMP limited;
STORE topHighest INTO 'output' USING PigStorage('\t');
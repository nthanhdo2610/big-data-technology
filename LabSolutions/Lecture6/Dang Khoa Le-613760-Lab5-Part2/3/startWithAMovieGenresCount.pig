REGISTER lib/piggybank-0.17.0.jar
movieBag = LOAD 'MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader AS
(movieId:chararray,title:chararray,genres:chararray);
start_with_a_movies = FILTER movieBag BY
org.apache.pig.piggybank.evaluation.string.LOWER(org.apache.pig.piggybank.evaluation.string.SUBSTRING(title,0,1)) == 'a';
tokenBag = FOREACH start_with_a_movies GENERATE TOKENIZE(genres,'|') AS genresBag;
genres = FOREACH tokenBag GENERATE flatten(genresBag);
genres_group = GROUP genres BY $0;
start_with_a_movies_genres_count = FOREACH genres_group GENERATE group, COUNT($1);
toDisplay = ORDER start_with_a_movies_genres_count BY $0;
STORE toDisplay INTO 'output' USING PigStorage('\t');

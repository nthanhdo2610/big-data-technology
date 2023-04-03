REGISTER lib/piggybank-0.17.0.jar;
movieBag = LOAD 'MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader AS
(movieId:int,title:chararray,genres:chararray);
ratingBag = LOAD 'MovieDataSet/rating.txt'USING PigStorage('\t') AS
(userId:chararray,movieId:int,rating:int,timestamp:chararray);
moviesBag_genresBag = FOREACH movieBag GENERATE movieId, title, TOKENIZE(genres,'|') AS genresBag;
flatten_moviesBag_generesBag = FOREACH moviesBag_genresBag GENERATE movieId, title, flatten($2);
adventure_movies = FILTER flatten_moviesBag_generesBag BY $2 == 'Adventure';
rating_adventure_movie_join = JOIN adventure_movies BY $0, ratingBag BY $1;
shorten_adventure_movies_with_rating = FOREACH rating_adventure_movie_join GENERATE $0, $2, $5, $1;
hihgest_rating_movies = DISTINCT (FILTER shorten_adventure_movies_with_rating BY $2 == 5);
top_20 = LIMIT (ORDER hihgest_rating_movies BY $0) 20;
prepaired_header_top_20 = FOREACH top_20 GENERATE $0 AS MovieId, $1 AS Genre, $2 AS Rating, $3 AS Title;
STORE prepaired_header_top_20 INTO 'output' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t','NO_MULTILINE','UNIX','WRITE_OUTPUT_HEADER');

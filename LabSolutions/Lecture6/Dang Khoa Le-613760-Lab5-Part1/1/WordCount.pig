lines = LOAD 'InputForWC.txt' USING TextLoader AS (line:chararray);
tokenBag = FOREACH lines GENERATE TOKENIZE(line, '\t ');
flatBag = FOREACH tokenBag GENERATE flatten($0);
words = GROUP flatBag BY token;
wordCounts = FOREACH words GENERATE group, COUNT(flatBag);
STORE wordCounts INTO 'output' USING PigStorage('|');

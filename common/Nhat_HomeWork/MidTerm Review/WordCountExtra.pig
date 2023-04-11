--pig -x local /home/cloudera/Desktop/WordCountExtra.pig
rmf /home/cloudera/Desktop/WCE_output

doc1 = LOAD '/home/cloudera/Desktop/WCE_input1.txt' AS (line: chararray);
doc2 = LOAD '/home/cloudera/Desktop/WCE_input2.txt' AS (line: chararray);

words1 = FOREACH doc1 GENERATE FLATTEN(TOKENIZE(line,' ')) AS word;
words2 = FOREACH doc2 GENERATE FLATTEN(TOKENIZE(line,' ')) AS word;

groupedWords1 = GROUP words1 BY word;
--groupedWords2 = GROUP words2 BY word;

wc = FOREACH groupedWords1 GENERATE group AS word, COUNT(words1);

dtm1 = DISTINCT words1;
dtm2 = DISTINCT words2;

joined = JOIN dtm1 BY word, dtm2 BY word;

count = FOREACH (GROUP joined ALL) GENERATE COUNT(joined);

--DUMP count;

STORE wc INTO '/home/cloudera/Desktop/WCE_output/wc' USING PigStorage(',');
STORE joined INTO '/home/cloudera/Desktop/WCE_output/join' USING PigStorage(',');
STORE count INTO '/home/cloudera/Desktop/WCE_output/count' USING PigStorage(',');

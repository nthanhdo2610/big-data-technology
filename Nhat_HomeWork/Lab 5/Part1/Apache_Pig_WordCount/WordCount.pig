--pig -x local /home/cloudera/Desktop/WordCount.pig
rmf /home/cloudera/Desktop/Lab5_part1_WC_output

inputLines = LOAD '/home/cloudera/Desktop/Lab5_part1_WC_input.txt' AS (line: chararray);

words = FOREACH inputLines GENERATE FLATTEN(TOKENIZE(line,' ')) AS word;

groupedWords = GROUP words BY word;

wordCount = FOREACH groupedWords GENERATE group, COUNT(words);

--DUMP wordCount;
STORE wordCount INTO '/home/cloudera/Desktop/Lab5_part1_WC_output' USING PigStorage(',');

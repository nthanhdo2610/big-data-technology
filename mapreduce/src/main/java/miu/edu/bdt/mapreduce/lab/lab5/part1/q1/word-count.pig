linesOfText = LOAD 'input/InputForWC.txt' using TextLoader AS (line:chararray);
flatBag = FOREACH linesOfText GENERATE FLATTEN(TOKENIZE(line, ' \t'));
grpd = GROUP flatBag BY ($0);
counts = FOREACH grpd GENERATE $0, COUNT($1);
--DUMP counts;
STORE counts INTO 'output' USING PigStorage('\t');
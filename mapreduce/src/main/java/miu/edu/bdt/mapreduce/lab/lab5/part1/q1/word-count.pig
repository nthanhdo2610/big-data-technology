linesOfText = LOAD 'input/InputForWC.txt' using TextLoader AS (line:chararray);
tokenBag = FOREACH linesOfText GENERATE TOKENIZE(line, ' \t') AS wordsBag;
flatBag = FOREACH tokenBag GENERATE flatten($0);
grpd = GROUP flatBag BY ($0);
counts = FOREACH grpd GENERATE $0, COUNT($1);
--DUMP counts;
STORE counts INTO 'output' USING PigStorage('\t');
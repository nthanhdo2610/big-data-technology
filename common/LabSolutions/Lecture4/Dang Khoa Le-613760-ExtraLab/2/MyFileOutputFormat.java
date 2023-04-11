package extra;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyFileOutputFormat extends TextOutputFormat{

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        Path originalFile = super.getDefaultWorkFile(context, extension);
        String fileName = originalFile.getName();
        if(fileName.endsWith("r-00000")){
            fileName = fileName.replace("part-r-00000", "StationTempRecord");
            originalFile = new Path(originalFile.getParent().toString()+File.separator+fileName);
        }
        return originalFile;
    }

    
}

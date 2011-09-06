package tiannan;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, VideoObject, Text, File> {
	
    public void reduce(Text key, Iterable<VideoObject> values, Context context) 
      throws IOException, InterruptedException {
        context.write(key, new File("ddd"));
    }	 
 }

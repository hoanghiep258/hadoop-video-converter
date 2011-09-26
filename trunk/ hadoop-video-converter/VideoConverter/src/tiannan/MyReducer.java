package tiannan;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import tiannan.input.VideoDivider;
import tiannan.output.Combiner;

public class MyReducer extends Reducer<Text, VideoObject, Text, File> {
	private Combiner combiner;
	private String url;
	
    public void reduce(Text key, Iterable<VideoObject> values, Context context) 
      throws IOException, InterruptedException {
    	url = "result.mov";   	
    	combiner = new Combiner(VideoDivider.getInfo(),url);
    	int count = 0;
    	byte[] temp = null; 
    	for (VideoObject vo: values){
    		if(count == 0){
    			temp = vo.getVideoByteArray();
    		}
    		else{
    			temp = combiner.concatenate(temp, vo.getVideoByteArray());
    		}
    		count++;
    	}
        context.write(key, new File(url));
    }	 
 }

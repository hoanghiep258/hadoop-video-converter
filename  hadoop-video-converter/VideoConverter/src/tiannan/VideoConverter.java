package tiannan;

        
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import tiannan.input.VideoInputFormat;
import tiannan.output.VideoOutputFormat;
        
public class VideoConverter {
	private static final Log LOG = LogFactory.getLog(VideoConverter.class);   
   
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
	    //For RND Lab cluster
	//    conf.set("hadoop.tmp.dir", "/home/hikaru/hadoop/hdfs");
	//    conf.set("mapred.local.dir", "/tmp/mapred");
	    
	    Job job = new Job(conf, "VideoConverter");
	    
	        
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VideoObject.class);
	        
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	        
	    job.setInputFormatClass(VideoInputFormat.class);
	    job.setOutputFormatClass(VideoOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    
	    Path outputPath = new Path(job.getWorkingDirectory().toString()+"/"+args[1]);
	    FileSystem fs = outputPath.getFileSystem(conf);
	    if(fs.exists(outputPath)){
	    	fs.delete(outputPath, true);
	    }
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
        
}
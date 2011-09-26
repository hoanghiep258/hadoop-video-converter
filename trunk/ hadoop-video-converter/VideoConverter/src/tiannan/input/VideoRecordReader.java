package tiannan.input;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.commons.logging.Log;


import tiannan.VideoObject;


public class VideoRecordReader extends RecordReader<Text, VideoObject>{
	
	private static final Log LOG = LogFactory.getLog(VideoRecordReader.class);
	private SequenceFileRecordReader fileRecordReader;
	private CompressionCodecFactory compressionCodecs = null;
	private VideoReader videoReader;
	private long start;	
	private long pos;
	private String filename;
	private long end;
	private int maxLineLength;
	
	private FSDataInputStream fileIn;
	private Text key = null;
	private VideoObject value = null;
	private VideoDivider videoDivider;

	@Override
	public synchronized void close() throws IOException {
	    if (videoReader != null) {
	    	videoReader.close(); 
	    }
	  }

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public VideoObject getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (start == end) {
		    return 0.0f;
		} 
		else {
		    return Math.min(1.0f, (pos - start) / (float)(end - start));
		}

	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		
	    FileSplit split = (FileSplit) genericSplit;
	    
	    Configuration job = context.getConfiguration();
	    
//	    
//	    fileRecordReader = new SequenceFileRecordReader<Text,VideoObject>();
//	    
//	    fileRecordReader.initialize(split, context);
	    	    
//	    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
//	                                    Integer.MAX_VALUE);
//	    start = split.getStart();
//	    end = start + split.getLength();
	    start = 0;
	    end = 5;
//	    LOG.info("Log__videoRecordReader FileName info: " + split.getLocations().toString());
	    
	    final Path file = split.getPath();
//	    compressionCodecs = new CompressionCodecFactory(job);

	    // open the file and seek to the start of the split
	    FileSystem fs = file.getFileSystem(job);
	    fileIn = fs.open(split.getPath());
	    
	    filename = split.getPath().getName().substring(0,split.getPath().getName().indexOf('.'));
	    
//	    LOG.info("Log__videoRecordReader NumReduceTasks: " + context.getNumReduceTasks());
	    
	    videoReader = new VideoReader(fileIn,job);
	    
	    //separate the video into five clips
	    videoDivider = new VideoDivider(end, videoReader.readVideoFile());

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	    if (start < end) {
	        key = new Text();	
	        Long temp = new Long(start);
	        key.set(filename+temp.toString());
	        value = new VideoObject(videoDivider.getNextClip(start));
	        LOG.info("Log__videoRecordReader ByteArrayLength: " + videoReader.getByteArrayLength());
	        start++;
	        return true;
	    }
	    else{
	    	return false;
	    }
	    
	}
}

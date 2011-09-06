package tiannan.output;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tiannan.VideoObject;


public class VideoOutputFormat extends FileOutputFormat<Text, VideoObject>{


	@Override
	public org.apache.hadoop.mapreduce.RecordWriter<Text, VideoObject> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new VideoRecordWriter();
	}

}

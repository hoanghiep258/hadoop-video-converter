package tiannan.output;


import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import tiannan.VideoObject;

public class VideoRecordWriter extends RecordWriter<Text, VideoObject>{

	private DataOutputStream out;
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		out.close();
	}

	@Override
	public void write(Text key, VideoObject value) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub

//	      boolean nullKey = key == null || key instanceof NullWritable;
//	      boolean nullValue = value == null || value instanceof NullWritable;
//	      if (nullKey && nullValue) {
//	        return;
//	      }
//	      if (!nullKey) {
//	        writeObject(key);
//	      }
//	      if (!(nullKey || nullValue)) {
//	        out.write(keyValueSeparator);
//	      }
//	      if (!nullValue) {
//	        writeObject(value);
//	      }
//	      out.write(newline);
		
	}
	
	private void writeObject(Object o) throws IOException {
//	      if (o instanceof Text) {
//	        Text to = (Text) o;
//	        out.write(to.getBytes(), 0, to.getLength());
//	      } else {
//	        out.write(o.toString().getBytes(utf8));
//	      }
	    }


}

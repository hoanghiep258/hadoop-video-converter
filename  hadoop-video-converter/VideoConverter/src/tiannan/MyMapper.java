package tiannan;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.IMediaWriter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;

public class MyMapper extends Mapper<Text, VideoObject, Text, VideoObject> {
 
	private static final Log LOG = LogFactory.getLog(MyMapper.class); 
    private Text videoName = new Text();
    private VideoObject mapperResult = null;    

    
    public void map(Text key, VideoObject value, Context context) throws IOException, InterruptedException {
   	
    	IContainer container = IContainer.make(); 	
    	  	
    	IContainerFormat containerFormat = IContainerFormat.make();
    	containerFormat.setInputFormat("mov");
   	
    	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.getVideoByteArray());
	 	LOG.info("Log__VideoConverter__byteArray: "+ byteArrayInputStream.available());
	 	
	 	container.setInputBufferLength(byteArrayInputStream.available());
    	container.open(byteArrayInputStream,containerFormat);
    	
    	//Read from file
//    	File temp = ByteArrayToFile(value.getVideoByteArray());
//    	IMediaReader reader = ToolFactory.makeReader(temp.getAbsolutePath());
	 	
    	//Read from inputStream
    	IMediaReader reader = ToolFactory.makeReader(container);
    	String videoOutputName = key.toString()+".mp4";
    	videoName.set(videoOutputName);
    	
    	
    	IMediaWriter writer = ToolFactory.makeWriter(videoOutputName,reader);
    	reader.addListener(writer);
    	while(reader.readPacket()==null);
    	 
    	
//    	video = new VideoObject(writer);    	
    	context.write(videoName, mapperResult);
    	
    }
    
	 public File ByteArrayToFile(byte[] in) throws IOException{
		 	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(in);
//		 	LOG.info("Log__VideoConverter__byteArray: "+ byteArrayInputStream.available());
	    	File newFile=new File("temp.mov");
	    	FileOutputStream fos = new FileOutputStream(newFile);
	    	int data; 
	    	while((data=byteArrayInputStream.read())!=-1){ 
		    	char ch = (char)data;
		    	fos.write(ch);
	    	}
	    	fos.flush();
	    	fos.close();
	    	return newFile;
	 }   
} 
package tiannan.input;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import com.sun.tools.javac.code.Attribute.Array;
import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IStreamCoder.Direction;

public class VideoDivider {
	private byte[] file;
	private long numOfClips = 1;	
	private long numPackets;
	
	private ArrayList<IStream> streams = new ArrayList<IStream>();
	private ArrayList<IStream> outputStreams = new ArrayList<IStream>();
	private IContainer container = IContainer.make();	
	private IContainer outContainer;
	
	private String format = "mov";
	private static HashMap<String, Integer> info = new HashMap<String, Integer>();
	
	private ArrayList<byte[]> outputClipsByteArray = new ArrayList<byte[]>();
	
	
	public VideoDivider(long clipNumber,byte[] file) throws IOException {
		// TODO Auto-generated constructor stub
		this.file = file;
		this.numOfClips = clipNumber;
		setInputContainer();
		separateVideo();
		closeInputContainer();
	}
	
	private void setInputContainer() throws IOException{
		
		//set input container
		ByteArrayInputStream bis = new ByteArrayInputStream(file);		
    	IContainerFormat containerFormat = IContainerFormat.make();
    	containerFormat.setInputFormat(format);
		container.setInputBufferLength(bis.available());   	
    	container.open(bis,containerFormat); 	
    	//Get Stream and Codec
    	int streamNumber = container.getNumStreams();    	
        for (int i = 0 ; i < streamNumber; i++){       	
        	streams.add(container.getStream(i));        	
        }
        numPackets = getVideoStream().getNumFrames();
        
        //for future combiner use
        this.setUpInfo();
	}
	
	private void closeInputContainer(){
		container.close();
	}
	
	private void separateVideo() throws IOException{		
		for(int i = 0 ; i < numOfClips; i++){      		
	    	outContainer = IContainer.make();
	    	outContainer.open("/Users/hikaru/Desktop/output/"+i+"."+format, IContainer.Type.WRITE, null);
	    	
	    	
	    	//set up output container and codec
	        for (IStream is : streams){
		         IStreamCoder vidCoder = is.getStreamCoder();	        	
		         IStreamCoder outStreamCoder = IStreamCoder.make(Direction.ENCODING, vidCoder.copyReference());	        	
		         IStream outStream = outContainer.addNewStream(is.getIndex());
		         outStream.setStreamCoder(outStreamCoder);
		         outputStreams.add(outStream);
	       }	
	        
	        //start to divide the video, write head, tailer and packets into output container
	       for(IStream is : outputStreams){
	    	    is.getStreamCoder().open();
    	   } 
	       outContainer.writeHeader();
    	   int count = 0;
           IPacket packet = IPacket.make();
           	
    	   while(container.readNextPacket(packet)>=0){
    	    	if (packet.isComplete()){    	    			 	  
    	    		int streamIndex = packet.getStreamIndex();
    		    	if (streamIndex != getVideoStream().getIndex()){
    		    		outContainer.writePacket(packet);
    		    	    	continue;
    		    	    }
    		    	    if(!packet.isKey()){
    		    	    	count++;
    		    	    }
    		    	    if(count == numPackets / numOfClips)
    		    	    	break;   		    	          
    		    	    outContainer.writePacket(packet);     		    	          
    	    	     }
    	   }//while
    	   outContainer.writeTrailer();
    	   for(IStream is : outputStreams){
    		   is.delete();
    	       is.getStreamCoder().close();
           } 
    	   outputStreams.clear();
    	   outContainer.close();
    	   FileInputStream file_input = new FileInputStream ("/Users/hikaru/Desktop/output/"+i+"."+format);
    	   byte[] temp = new byte[file_input.available()];
    	   file_input.read(temp);
    	   outputClipsByteArray.add(temp);
    	   file_input.close();
      	}//for  		   		
	}
	
	private IStream getVideoStream(){
		IStream videoStream = null;
		for (IStream is : streams){
			if(is.getStreamCoder().getCodec().getType().equals(ICodec.Type.CODEC_TYPE_VIDEO)){
				videoStream = is;
				break;
			}
		}
		return videoStream;
	}
	
	private void setUpInfo(){
		for (IStream is : streams){
			if(is.getStreamCoder().getCodec().getType().equals(ICodec.Type.CODEC_TYPE_VIDEO)){
				IStreamCoder coder = is.getStreamCoder();
				info.put("videoStreamIndex",is.getIndex());
				info.put("videoStreamId",is.getId());
				info.put("width",coder.getWidth());
				info.put("height",coder.getHeight());
			}
			else if(is.getStreamCoder().getCodec().getType().equals(ICodec.Type.CODEC_TYPE_AUDIO)){
				IStreamCoder coder = is.getStreamCoder();
				info.put("audioStreamIndex",is.getIndex());
				info.put("audioStreamId",is.getId());
				info.put("channelCount",coder.getChannels());
				info.put("sampleRate",coder.getSampleRate());
			}
		}
	}
	
	public byte[] getNextClip(long index){
		return outputClipsByteArray.get((int)index);
	}
	
	public String getClipName(long index){
		return null;
	}
	
	public static HashMap<String, Integer> getInfo(){
		return info;
	}
}

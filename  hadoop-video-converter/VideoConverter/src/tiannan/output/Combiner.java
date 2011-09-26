package tiannan.output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import javax.imageio.stream.FileImageInputStream;

import org.apache.hadoop.mapred.FileInputFormat;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.IMediaViewer;
import com.xuggle.mediatool.IMediaWriter;
import com.xuggle.mediatool.MediaToolAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.AudioSamplesEvent;
import com.xuggle.mediatool.event.IAddStreamEvent;
import com.xuggle.mediatool.event.IAudioSamplesEvent;
import com.xuggle.mediatool.event.ICloseCoderEvent;
import com.xuggle.mediatool.event.ICloseEvent;
import com.xuggle.mediatool.event.IOpenCoderEvent;
import com.xuggle.mediatool.event.IOpenEvent;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.mediatool.event.VideoPictureEvent;
import com.xuggle.xuggler.IAudioSamples;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.IVideoPicture;

import static java.lang.System.out;
import static java.lang.System.exit;


public class Combiner
{
	//video parameters
    private int videoStreamIndex = 0;
    private int videoStreamId = 0;
    private int width = 480;
    private int height = 360;
    // audio parameters
    private int audioStreamIndex = 1;
    private int audioStreamId = 0;
    private int channelCount = 2;
    private int sampleRate = 44100; // Hz
    
    private IContainerFormat containerFormat = IContainerFormat.make();
    private String destinationURl;
  
	public Combiner(HashMap<String,Integer> setUpInfo, String URL) {
		// TODO Auto-generated constructor stub
		// set up the output file format
		containerFormat.setInputFormat("mp4");
		
		if(!setUpInfo.isEmpty()){
			videoStreamIndex = setUpInfo.get("videoStreamIndex");
			videoStreamId = setUpInfo.get("videoStreamId");
			width = setUpInfo.get("width");
			height = setUpInfo.get("height");
			
			audioStreamIndex = setUpInfo.get("audioStreamIndex");
			audioStreamId = setUpInfo.get("audioStreamId");
			channelCount = setUpInfo.get("channelCount");
			sampleRate = setUpInfo.get("sampleRate");
			
			destinationURl = URL;
		}
		else{
		   //log error
		}
	}


  public byte[] concatenate(byte[] sourceUrl1, byte[] sourceUrl2)
  {

    // create the first media reader
	  
	IContainer container1 = IContainer.make();
	ByteArrayInputStream byteArrayInputStream1 = new ByteArrayInputStream(sourceUrl1);	
 	container1.setInputBufferLength(byteArrayInputStream1.available());
	container1.open(byteArrayInputStream1,containerFormat);
    IMediaReader reader1 = ToolFactory.makeReader(container1);

    // create the second media reader
    IContainer container2 = IContainer.make();
	ByteArrayInputStream byteArrayInputStream2 = new ByteArrayInputStream(sourceUrl2);	
 	container1.setInputBufferLength(byteArrayInputStream2.available());
	container1.open(byteArrayInputStream2,containerFormat);
    IMediaReader reader2 = ToolFactory.makeReader(container2);

    // create the media concatenator

    MediaConcatenator concatenator = new MediaConcatenator(audioStreamIndex,
      videoStreamIndex);

    // concatenator listens to both readers

    reader1.addListener(concatenator);
    reader2.addListener(concatenator);

    // create the media writer which listens to the concatenator

    IMediaWriter writer = ToolFactory.makeWriter(destinationURl);
    concatenator.addListener(writer);

    // add the video stream

    writer.addVideoStream(videoStreamIndex, videoStreamId, width, height);

    // add the audio stream

    writer.addAudioStream(audioStreamIndex, audioStreamId, channelCount,
      sampleRate);

    // read packets from the first source file until done

    while (reader1.readPacket() == null)
      ;

    // read packets from the second source file until done

    while (reader2.readPacket() == null)
      ;

    // close the writer

    writer.close();
    byte[] result = null;
    try {
    	FileImageInputStream fis = new FileImageInputStream(new File(destinationURl));
    	result = new byte[(int) fis.length()];
		fis.read(result);
		
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return result;
  }
  
  static class MediaConcatenator extends MediaToolAdapter
  {
    // the current offset
    
    private long mOffset = 0;
    
    // the next video timestamp
    
    private long mNextVideo = 0;
    
    // the next audio timestamp
    
    private long mNextAudio = 0;

    // the index of the audio stream
    
    private final int mAudoStreamIndex;
    
    // the index of the video stream
    
    private final int mVideoStreamIndex;
    
    public MediaConcatenator(int audioStreamIndex, int videoStreamIndex)
    {
      mAudoStreamIndex = audioStreamIndex;
      mVideoStreamIndex = videoStreamIndex;
    }
    
    public void onAudioSamples(IAudioSamplesEvent event)
    {
      IAudioSamples samples = event.getAudioSamples();
      long newTimeStamp = samples.getTimeStamp() + mOffset;
      mNextAudio = samples.getNextPts();
      samples.setTimeStamp(newTimeStamp);
      super.onAudioSamples(new AudioSamplesEvent(this, samples,
        mAudoStreamIndex));
    }

    public void onVideoPicture(IVideoPictureEvent event)
    {
      IVideoPicture picture = event.getMediaData();
      long originalTimeStamp = picture.getTimeStamp();
      long newTimeStamp = originalTimeStamp + mOffset;    
      mNextVideo = originalTimeStamp + 1;

      picture.setTimeStamp(newTimeStamp);
      super.onVideoPicture(new VideoPictureEvent(this, picture,
        mVideoStreamIndex));
    }
    
//    public void onClose(ICloseEvent event)
//    {
//      mOffset = Math.max(mNextVideo, mNextAudio);
//
//      if (mNextAudio < mNextVideo)
//      {
//      }
//    }
//
//    public void onAddStream(IAddStreamEvent event)
//    {
//      // overridden to ensure that add stream events are not passed down
//      // the tool chain to the writer, which could cause problems
//    }
//
//    public void onOpen(IOpenEvent event)
//    {
//      // overridden to ensure that open events are not passed down the tool
//      // chain to the writer, which could cause problems
//    }
//
//    public void onOpenCoder(IOpenCoderEvent event)
//    {
//      // overridden to ensure that open coder events are not passed down the
//      // tool chain to the writer, which could cause problems
//    }
//
//    public void onCloseCoder(ICloseCoderEvent event)
//    {
//      // overridden to ensure that close coder events are not passed down the
//      // tool chain to the writer, which could cause problems
//    }
  }
}

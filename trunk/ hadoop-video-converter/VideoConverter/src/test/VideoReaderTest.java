package test;


import java.io.IOException;

import tiannan.input.VideoReader;
import junit.framework.TestCase;

public class VideoReaderTest extends TestCase {
	private VideoReader reader = null;
	
	public VideoReaderTest(){
//		reader = new VideoReader();
	}
	
	public void testReadVideoFile() throws IOException{
		assertNotNull(reader.readVideoFile());
	}

}

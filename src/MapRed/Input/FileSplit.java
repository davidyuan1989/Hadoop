package MapRed.Input;

import java.io.IOException;
import java.io.Serializable;

import Utility.JZFile;

public class FileSplit implements IInputSplit, Serializable {

	private static final long serialVersionUID = 8L;
	
	private JZFile file;
	private long start;
	private long length;
	
	public FileSplit(JZFile file, long start, long length) {
		this.file = file;
		this.start = start;
		this.length = length;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return length;
	}
	
	public JZFile getFile() {
		return file;
	}
	
	public long getStartPos() {
		return start;
	}

}

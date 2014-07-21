package MapRed.Output;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import MapRed.Task.TaskContext;
import Utility.JZFile;
import Utility.JZSequenceFile;

public class TextRecordWriter implements IRecordWriter<String, String> {

	private JZSequenceFile file; 
	private BufferedWriter writer;
	
	public TextRecordWriter(JZSequenceFile file) {
		this.file = file;
		writer = new BufferedWriter(new OutputStreamWriter(file.getOutputStream()));
	}
	
	@Override
	public void write(String key, String value) throws IOException,
			InterruptedException {
		writer.write(key);
		writer.write(value);
	}

	@Override
	public void close(TaskContext context) throws IOException,
			InterruptedException {
		writer.close();
	}

	@Override
	public JZFile getOutputFile() {
		return file;
	}

}

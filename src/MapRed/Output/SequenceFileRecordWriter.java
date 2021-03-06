package MapRed.Output;

import java.io.IOException;

import MapRed.Task.TaskContext;
import Utility.JZFile;
import Utility.JZSequenceFile;

public class SequenceFileRecordWriter<K, V> implements IRecordWriter<K, V>{
	
	private JZSequenceFile file;
	private JZSequenceFile.Writer<K, V> writer;

	public SequenceFileRecordWriter(JZSequenceFile file) {
		this.file = file;
		writer = new JZSequenceFile.Writer<K, V>(file);
	}
	
	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		writer.write(key, value);
	}

	@Override
	public void close(TaskContext context) throws IOException,
			InterruptedException {
		if (writer != null) {
			writer.close();
		}
	}

	@Override
	public JZFile getOutputFile() {
		return file;
	}
}

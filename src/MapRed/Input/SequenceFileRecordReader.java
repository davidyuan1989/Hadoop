package MapRed.Input;

import java.io.IOException;

import MapRed.Task.TaskContext;
import Utility.JZSequenceFile;

public class SequenceFileRecordReader<K, V> implements IRecordReader<K, V>{

	public JZSequenceFile file;
	private JZSequenceFile.Reader<K, V> reader;
	
	public SequenceFileRecordReader(JZSequenceFile file) {
		this.file = file;
		reader = new JZSequenceFile.Reader<K, V>(file);
	}
	
	@Override
	public void initialize(IInputSplit split, TaskContext context)
			throws IOException, InterruptedException {
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}

	@Override
	public K getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

}

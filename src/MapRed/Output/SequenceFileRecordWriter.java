package MapRed.Output;

import java.io.IOException;
import java.io.ObjectOutputStream;

import MapRed.Task.TaskContext;
import Utility.JZSequenceFile;

public class SequenceFileRecordWriter<K, V> implements IRecordWriter<K, V>{
	
	private JZSequenceFile<K, V> file;
	private ObjectOutputStream outStream;

	public SequenceFileRecordWriter(JZSequenceFile<K, V> file) {
		this.file = file;
		try {
			outStream = new ObjectOutputStream(file.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		outStream.writeObject(key);
		outStream.writeObject(value);
		file.numOfEntries++;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void close(TaskContext context) throws IOException,
			InterruptedException {
		if (outStream != null) {
			outStream.close();
		}
	}

}

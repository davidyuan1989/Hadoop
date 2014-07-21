package MapRed.Output;

import java.io.IOException;
import java.util.Comparator;

import MapRed.Task.TaskContext;
import Utility.JZFile;
import Utility.JZSequenceFile;

public class SequenceFileRecordSortedWriter<K, V> implements IRecordWriter<K, V>{

	private JZSequenceFile file;
	private JZSequenceFile.Writer<K, V> writer;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public SequenceFileRecordSortedWriter(JZSequenceFile file, Comparator comparator, String id) {
		this.file = file;
		writer = new JZSequenceFile.Writer<K, V>(file);
		writer.setup(comparator, id);
	}
	
	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		writer.sortedWrite(key, value);
	}

	@Override
	public void close(TaskContext context) throws IOException,
			InterruptedException {
		writer.sortedClose();
	}

	@Override
	public JZFile getOutputFile() {
		return file;
	}

}

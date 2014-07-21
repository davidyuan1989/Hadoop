package MapRed.Output;

import java.io.IOException;

import MapRed.Task.TaskContext;
import Utility.JZFile;

public interface IRecordWriter<K, V> {
	public void write(K key, V value) throws IOException, InterruptedException;
	public JZFile getOutputFile();
	public void close(TaskContext context) throws IOException, InterruptedException;
}

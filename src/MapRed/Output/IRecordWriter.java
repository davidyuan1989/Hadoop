package MapRed.Output;

import java.io.IOException;

import MapRed.Task.TaskContext;

public interface IRecordWriter<K, V> {
	public void write(K key, V value) throws IOException, InterruptedException;
 
	@SuppressWarnings("rawtypes")
	public void close(TaskContext context) throws IOException, InterruptedException;
}

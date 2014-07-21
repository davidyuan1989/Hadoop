package MapRed.Output;

import java.io.IOException;

import MapRed.Task.TaskContext;
import Utility.JZFile;

public interface IOutputFormat<K, V> {
	public IRecordWriter<K, V> getRecordWriter(JZFile file, TaskContext context
			) throws IOException, InterruptedException;
}

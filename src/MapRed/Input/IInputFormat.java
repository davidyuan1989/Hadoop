package MapRed.Input;

import java.io.IOException;
import java.util.List;

import MapRed.Job.JobContext;
import MapRed.Task.TaskContext;

public interface IInputFormat<K, V> {
	public List<IInputSplit> getSplits(JobContext job) throws IOException, InterruptedException;

	public IRecordReader<K, V> createIRecordReader(IInputSplit split,
	                                     TaskContext job) throws IOException, InterruptedException;
}

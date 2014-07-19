package MapRed.Input;

import java.io.IOException;

import MapRed.Task.TaskContext;

public class TextInputFormat extends FileInputFormat<Long, String> {

	@SuppressWarnings("rawtypes")
	@Override
	public IRecordReader<Long, String> createIRecordReader(IInputSplit split,
			TaskContext job) throws IOException, InterruptedException {
		return new LineRecordReader();
	}

}

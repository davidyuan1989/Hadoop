package MapRed.Input;

import java.io.IOException;

import MapRed.Task.TaskContext;

public class TextInputFormat extends FileInputFormat<Long, String> {

	@Override
	public IRecordReader<Long, String> createIRecordReader(IInputSplit split,
			TaskContext job) throws IOException, InterruptedException {
		IRecordReader<Long, String> reader = new LineRecordReader();
		reader.initialize(split, job);
		return reader;
	}

}

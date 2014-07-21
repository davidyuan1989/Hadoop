package MapRed.Output;

import java.io.IOException;

import MapRed.Task.TaskContext;
import Utility.JZFile;
import Utility.JZSequenceFile;

public class SequenceFileOutputFormat implements IOutputFormat<String, String> {

	@Override
	public IRecordWriter<String, String> getRecordWriter(JZFile file, TaskContext context)
			throws IOException, InterruptedException {
		JZSequenceFile JZfile = (JZSequenceFile) file;
		return new TextRecordWriter(JZfile);
	}
}

package MapRed.Input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import MapRed.Job.JobContext;
import Utility.JZFile;

public abstract class FileInputFormat<K, V> implements IInputFormat<K, V>{

	public List<IInputSplit> getSplits(JobContext job) throws IOException, InterruptedException {

		long minSize = Math.max(getFormatMinSplitSize(), job.getMinSplitSize());
		long maxSize = job.getMaxSplitSize();

		List<IInputSplit> resultList = new ArrayList<IInputSplit>();
		List<JZFile> inputFiles = job.getInputFiles();
		
		for (JZFile file : inputFiles) {
			long fileSize = file.size;
			
			if (fileSize != 0) {
				long splitSize = Math.max(maxSize, minSize);
				long remainingSize = fileSize;
				
				while (remainingSize >= splitSize) {
					resultList.add(new FileSplit(file, fileSize-remainingSize, splitSize));
					remainingSize -= splitSize;
				}
				
				if (remainingSize != 0) {
					resultList.add(new FileSplit(file, fileSize-remainingSize, remainingSize));
				}
			}
			else {
				resultList.add(new FileSplit(file, 0, fileSize));
			}
		}

		return resultList;
	}

	/**
	 * Get the lower bound on split size imposed by the format.
	 * @return the number of bytes of the minimal split for this format
	 */
	protected long getFormatMinSplitSize() {
		return 1;
	}
}

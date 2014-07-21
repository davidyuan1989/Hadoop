package MapRed.Input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import MapRed.Task.TaskContext;
import Utility.JZFile;

public class LineRecordReader implements IRecordReader<Long, String>{

	private final int carriageReturnLength = 2;
	
	private long start;				/* Start position of the file */
	private long pos;				/* Current position in the file */
	private long end;				/* The end position of the block to read */
	private BufferedReader reader;	/* Buffered reader */
	private long key;				/* Key of the record. Here it's the same as pos */
	private String value;			/* Value of the record. Here it's the line of text */

	@Override
	public void initialize(IInputSplit split, TaskContext context)
			throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) split;
		start = fileSplit.getStartPos();
		pos = start;
		end = pos + fileSplit.getLength();

		JZFile file = fileSplit.getFile();
		reader = new BufferedReader(new InputStreamReader(file.getInputStream()));
		reader.skip(pos);

		/* Skip the first line */
		if (pos != 0) {
			String temp = reader.readLine();
			int lengthRead = temp.length();
			//test
			//System.out.println("Skipped key value read: " + pos + " " + temp);
			
			pos += lengthRead + carriageReturnLength;
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		int lengthRead = 0;		/* Length of the line read */

		/* Read line and skip empty line */
		while (pos < end && lengthRead == 0) {
			/* Get the key and value */
			key = pos;
			value = reader.readLine();

			/* Check validity */
			if (value == null) {
				return false;
			}

			lengthRead = value.length();
			pos += lengthRead + carriageReturnLength;
		}

		//test
		//System.out.println("Last key value read: " + key + " " + value);
		
		/* End of the block or end of file */
		if (lengthRead == 0) {
			return false;
		}
		else {
			return true;
		}
	}

	@Override
	public Long getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public String getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float)(end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

}

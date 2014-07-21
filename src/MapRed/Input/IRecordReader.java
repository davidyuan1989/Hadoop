package MapRed.Input;

import java.io.IOException;

import MapRed.Task.TaskContext;

public interface IRecordReader<K, V> {
	/**
	 * Called once at initialization.
	 * @param split the split that defines the range of records to read
	 * @param context the information about the task
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void initialize(IInputSplit split,
			TaskContext context
			) throws IOException, InterruptedException;

	/**
	 * Read the next key, value pair.
	 * @return true if a key/value pair was read
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException;

	/**
	 * Get the current key
	 * @return the current key or null if there is no current key
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public K getCurrentKey() throws IOException, InterruptedException;

	/**
	 * Get the current value.
	 * @return the object that was read
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public V getCurrentValue() throws IOException, InterruptedException;

	/**
	 * The current progress of the record reader through its data.
	 * @return a number between 0.0 and 1.0 that is the fraction of the data read
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public float getProgress() throws IOException, InterruptedException;

	/**
	 * Close the record reader.
	 */
	public void close() throws IOException;
}

package MapRed.Input;

import java.io.IOException;

public interface IInputSplit {

	/**
	 * Get the size of the split, so that the input splits can be sorted by size.
	 * @return the number of bytes in the split
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public long getLength() throws IOException, InterruptedException;

}

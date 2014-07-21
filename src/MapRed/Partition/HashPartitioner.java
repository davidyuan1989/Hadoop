package MapRed.Partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import Utility.JZFile;
import Utility.JZSequenceFile;
import Utility.Utility;

public class HashPartitioner<K, V> implements IPartitioner<K, V>{

	@Override
	public int getPartition(K key, V value, int numPartitions) {
		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

	@SuppressWarnings("unchecked")
	public List<JZSequenceFile> partition(List<String> inputFiles, 
			int numPartitions, Comparator<K> comparator, String jobTaskTrackerID) {

		final Comparator<K> tempComparator = comparator;

		/* Input files */
		List<JZSequenceFile> inFiles = 
				new ArrayList<JZSequenceFile>();

		/* Create the queue used to find the least key */
		Comparator<Object[]> com = new Comparator<Object[]>() {
			@Override
			public int compare(Object[] o1, Object[] o2) {
				return tempComparator.compare((K) o1[0], (K) o2[0]);
			}
		};	
		PriorityQueue<Object[]> keyQueue = new PriorityQueue<Object[]>(com);

		/* Initialize the queue */
		for (int i = 0; i < inputFiles.size(); i++) {
			JZSequenceFile file = 
					new JZSequenceFile(JZFile.LocalFileSystem, inputFiles.get(i), 0, 0);
			inFiles.add(file);
			JZSequenceFile.Reader<K, List<V>> reader = new JZSequenceFile.Reader<>(file);
			if (reader.nextKeyValue()) {
				Object[] entry = new Object[]{reader.getCurrentKey(), reader};
				keyQueue.offer(entry);
			}
		}

		/* Output file writers */
		List<JZSequenceFile> outFiles = 
				new ArrayList<JZSequenceFile>();
		List<JZSequenceFile.Writer<K, List<V>>> writers = 
				new ArrayList<JZSequenceFile.Writer<K, List<V>>>();

		/* Initialize the writers */
		for (int i = 0; i < numPartitions; i++) {
			String fileName = createPartitionFileName(jobTaskTrackerID, i);
			JZSequenceFile file = 
					new JZSequenceFile(JZFile.LocalFileSystem, fileName, 0, 0);
			outFiles.add(file);
			writers.add(new JZSequenceFile.Writer<K, List<V>>(file));
		}

		/* Get the first least key */
		List<V> list = new ArrayList<V>();
		Object[] firstEntry = keyQueue.peek();
		K preKey = (K) firstEntry[0];
		JZSequenceFile.Reader<K, List<V>> reader = (JZSequenceFile.Reader<K, List<V>>) firstEntry[1];

		/* Traverse all the sorted block files to get the lists for all keys and sort them */
		while (!keyQueue.isEmpty()) {
			/* Get the least key and corresponding value */
			Object[] entry = keyQueue.poll();
			K key = (K) entry[0];
			reader = (JZSequenceFile.Reader<K, List<V>>) entry[1];
			List<V> value = (List<V>) reader.getCurrentValue();

			/* Add to the list or create a new list */
			if (preKey.equals(key)) {
				list.addAll(value);
			}
			else {
				writers.get(getPartition(preKey, null, numPartitions)).write(preKey, list);

				//test
				System.out.print("\n" + preKey + ":");
				for (int i = 0; i < list.size(); i++) {
					System.out.print(" " + list.get(i));
				}
				
				preKey = key;
				list = new ArrayList<V>(value);
			}

			/* Read a new value from the corresponding block where the previous value is read */
			if (reader.nextKeyValue()) {
				K newKey = (K) reader.getCurrentKey();
				entry[0] = newKey;
				keyQueue.offer(entry);
			}
			else {
				reader.close();
			}
		}
		
		if (!list.isEmpty()) {
			writers.get(getPartition(preKey, null, numPartitions)).write(preKey, list);
			
			//test
			System.out.print(preKey + ":");
			for (int i = 0; i < list.size(); i++) {
				System.out.print(" " + list.get(i));
			}
			System.out.println("\n");
		}

		/* Close the writers */
		for (int i = 0; i < numPartitions; i++) {
			writers.get(i).close();
		}
		
		return outFiles;
	}

	public static String createPartitionFileName(String jobTaskTrackerID, int partitionID) {
		String fileName = Utility.PartitionOutputNameBase + "_" + jobTaskTrackerID + "_" + 
						String.valueOf(partitionID) + ".dat";
		return fileName;
	}
}

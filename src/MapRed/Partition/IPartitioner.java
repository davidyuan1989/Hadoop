package MapRed.Partition;


public interface IPartitioner<K, V> {
	/** 
	 * Get the partition number for a given key (hence record) given the total 
	 * number of partitions i.e. number of reduce-tasks for the job.
	 *   
	 * <p>Typically a hash function on a all or a subset of the key.</p>
	 *
	 * @param key the key to be partioned.
	 * @param value the entry value.
	 * @param numPartitions the total number of partitions.
	 * @return the partition number for the <code>key</code>.
	 */
	public int getPartition(K key, V value, int numPartitions);
}

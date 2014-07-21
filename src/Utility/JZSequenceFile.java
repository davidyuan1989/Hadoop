package Utility;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class JZSequenceFile extends JZFile {

	private static final long serialVersionUID = 10L;
	
	public int numOfEntries;		/* Number of entries stored in this sequence file */
	
	public JZSequenceFile(int fileSystem, String path, long size, int numOfEntries) {
		super(fileSystem, path, size);
		this.numOfEntries = numOfEntries;
	}

	/* Sub Reader class used to read from sequence file */
	public static class Reader<K, V> {
		
		private ObjectInputStream inStream;
		private K key;
		private V value;
		
		public Reader(JZSequenceFile file) {
			try {
				inStream = new ObjectInputStream(file.getInputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@SuppressWarnings("unchecked")
		public boolean nextKeyValue() {
			try {
				key = (K) inStream.readObject();
				value = (V) inStream.readObject();
				return true;
			} catch (EOFException e) {
				System.out.println("\nEnd of file.");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}
		
		public K getCurrentKey() {
			return key;
		}
		
		public V getCurrentValue() {
			return value;
		}
		
		public void close() {
			try {
				inStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* Sub Writer class used to write to sequence file */
	public static class Writer<K, V> {
		
		private final int BUFSIZE = 65536;
		private final int DIMENSION = 2;
		private final String fileNameBase = "sortedblock";
		private final String fileTypeSuffix = ".dat";
		
		private String id;
		private JZSequenceFile file;
		private ObjectOutputStream outStream;
		private Comparator<K> comparator;
		private PriorityQueue<Object[]> buffer;
		private List<String> files;
		private int fileIndexPool;
		
		public Writer(JZSequenceFile file) {
			try {
				this.file = file;
				outStream = new ObjectOutputStream(file.getOutputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void write(K key, V value) {
			try {
				outStream.writeObject(key);
				outStream.writeObject(value);
				file.numOfEntries++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void write(K key, List<V> valueList) {
			try {
				outStream.writeObject(key);
				outStream.writeObject(valueList);
				file.numOfEntries++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void sortedWrite(K key, V value) {
			if (comparator == null) {
				write(key, value);
				return;
			}
			
			Object[] entry = new Object[DIMENSION];
			entry[0] = key;
			entry[1] = value;
			
			buffer.offer(entry);
			
			if (buffer.size() >= BUFSIZE) {
				writeFile();
			}
		}
		
		public void setup(Comparator<K> comp, String id) {
			
			/* Set comparator and initialize the priority queue */
			this.id = id;
			this.comparator = comp;
			Comparator<Object[]> com = new Comparator<Object[]>() {
				@SuppressWarnings("unchecked")
				@Override
				public int compare(Object[] o1, Object[] o2) {
					return comparator.compare((K) o1[0], (K) o2[0]);
				}
			};
			buffer = new PriorityQueue<Object[]>(com);
			
			/* Initialize the files list */
			files = new ArrayList<String>();
			fileIndexPool = 0;
		}
		
		private void writeFile() {
			try {
				String fileName = Utility.LocalFStemp + fileNameBase + "_" + id + "_" +
						String.valueOf(fileIndexPool++) + fileTypeSuffix;
				files.add(fileName);
				
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(new File(fileName)));
				while (!buffer.isEmpty()) {
					Object[] entry = buffer.poll();
					outputStream.writeObject(entry[0]);
					outputStream.writeObject(entry[1]);
					
					//test
					System.out.print(entry[0] + " ");
					System.out.println(entry[1]);
				}
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		@SuppressWarnings("unchecked")
		public void merge() {
			
			/* Write the rest data stored in buffer to a sorted block file */
			if (!buffer.isEmpty()) {
				writeFile();
			}
			
			/* Create the queue used to find the least key */
			Comparator<Object[]> com = new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					comparator.compare((K) o1[0], (K) o2[0]);
					return 0;
				}
			};	
			PriorityQueue<Object[]> keyQueue = new PriorityQueue<Object[]>(com);
			
			/* Open all the sorted block files */
			for (int i = 0; i < files.size(); i++) {
				try {
					ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(new File(files.get(i))));
					Object[] entry = new Object[]{inStream.readObject(), inStream};
					keyQueue.offer(entry);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
			
			/* Get the first least key */
			List<V> list = new ArrayList<V>();
			Object[] firstEntry = keyQueue.peek();
			K preKey = (K) firstEntry[0];
			ObjectInputStream inputStream = (ObjectInputStream) firstEntry[1];
			
			/* Traverse all the sorted block files to get the lists for all keys and sort them */
			while (!keyQueue.isEmpty()) {
				try {
					/* Get the least key and corresponding value */
					Object[] entry = keyQueue.poll();
					K key = (K) entry[0];
					inputStream = (ObjectInputStream) entry[1];
					V value = (V) inputStream.readObject();
					
					/* Add to the list or create a new list */
					if (preKey.equals(key)) {
						list.add(value);
					}
					else {
						write(preKey, list);
						
						//test
						System.out.print("\n" + preKey + ":");
						for (int i = 0; i < list.size(); i++) {
							System.out.print(" " + list.get(i));
						}
						
						preKey = key;
						list = new ArrayList<V>();
						list.add(value);
					}
					
					/* Read a new value from the corresponding block where the previous value is read */
					K newKey = (K) inputStream.readObject();
					entry[0] = newKey;
					keyQueue.offer(entry);
					
				} catch (EOFException e) {
					/* End of file, close this sorted block file */
					try {
						inputStream.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					System.out.println("\nEnd of file.");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (!list.isEmpty()) {
				write(preKey, list);
				
				//test
				System.out.print(preKey + ":");
				for (int i = 0; i < list.size(); i++) {
					System.out.print(" " + list.get(i));
				}
				System.out.println("\n");
			}
			
			/* Delete all the opened sorted blocks */
			/*
			for (int i = 0; i < files.size(); i++) {
				File file = new File(files.get(i));
				file.delete();
			}
			*/
		}
		
		public void close() {
			try {
				if (outStream != null) {
					outStream.close();
				}
				buffer = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void sortedClose() {
			try {
				if (outStream != null) {
					merge();
					outStream.close();
				}
				buffer = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void staticMerge(List<String> files, JZSequenceFile file, Comparator comparator) {
		
		final Comparator tempComparator = comparator;

		/* Input files */
		List<JZSequenceFile> inFiles = 
				new ArrayList<JZSequenceFile>();
		
		/* Create the queue used to find the least key */
		Comparator<Object[]> com = new Comparator<Object[]>() {
			@Override
			public int compare(Object[] o1, Object[] o2) {
				return tempComparator.compare(o1[0], o2[0]);
			}
		};	
		PriorityQueue<Object[]> keyQueue = new PriorityQueue<Object[]>(com);

		/* Initialize the queue */
		for (int i = 0; i < files.size(); i++) {
			JZSequenceFile infile = 
					new JZSequenceFile(JZFile.LocalFileSystem, files.get(i), 0, 0);
			inFiles.add(infile);
			JZSequenceFile.Reader<Object, List<Object>> reader = 
					new JZSequenceFile.Reader<Object, List<Object>>(infile);
			if (reader.nextKeyValue()) {
				Object[] entry = new Object[]{reader.getCurrentKey(), reader};
				keyQueue.offer(entry);
			}
		}

		/* Output file writers */
		JZSequenceFile.Writer<Object, List<Object>> writer = 
				new JZSequenceFile.Writer<Object, List<Object>>(file);

		/* Get the first least key */
		List<Object> list = new ArrayList<Object>();
		Object[] firstEntry = keyQueue.peek();
		Object preKey = (Object) firstEntry[0];
		JZSequenceFile.Reader<Object, List<Object>> reader = 
				(JZSequenceFile.Reader<Object, List<Object>>) firstEntry[1];

		/* Traverse all the sorted block files to get the lists for all keys and sort them */
		while (!keyQueue.isEmpty()) {
			/* Get the least key and corresponding value */
			Object[] entry = keyQueue.poll();
			Object key = (Object) entry[0];
			reader = (JZSequenceFile.Reader<Object, List<Object>>) entry[1];
			List<Object> value = (List<Object>) reader.getCurrentValue();

			/* Add to the list or create a new list */
			if (preKey.equals(key)) {
				list.addAll(value);
			}
			else {
				writer.write(preKey, list);

				//test
				System.out.print("\n" + preKey + ":");
				for (int i = 0; i < list.size(); i++) {
					System.out.print(" " + list.get(i));
				}
				
				preKey = key;
				list = new ArrayList<Object>(value);
			}

			/* Read a new value from the corresponding block where the previous value is read */
			if (reader.nextKeyValue()) {
				Object newKey = (Object) reader.getCurrentKey();
				entry[0] = newKey;
				keyQueue.offer(entry);
			}
			else {
				reader.close();
			}
		}
		
		if (!list.isEmpty()) {
			writer.write(preKey, list);
			
			//test
			System.out.print(preKey + ":");
			for (int i = 0; i < list.size(); i++) {
				System.out.print(" " + list.get(i));
			}
			System.out.println("\n");
		}

		/* Close the writers */
		writer.close();
	}
}

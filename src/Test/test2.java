package Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import JZFS.JZFSFileInputStream;
import JZFS.JZFSFileOutputStream;
import Utility.Utility;

public class test2 {

	public static void main(String[] args) {
		try {
			Utility.configure();

			byte[] data = new byte[1024];
			int bytes = 0;

			FileInputStream inStream = new FileInputStream(new File("input02.txt"));
			JZFSFileOutputStream outputStream = new JZFSFileOutputStream("JZFS/input/input02.txt");

			while ((bytes = inStream.read(data)) != -1) {
				outputStream.write(data, 0, bytes);
			}

			inStream.close();
			outputStream.close();
			
			System.out.println("Finish adding files. Now read back the file.");
			
			JZFSFileInputStream in = new JZFSFileInputStream("JZFS/input/input02.txt");
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

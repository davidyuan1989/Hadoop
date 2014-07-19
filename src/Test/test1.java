package Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import JZFS.JZFSFileOutputStream;
import Utility.Utility;

public class test1 {

	public static void main(String[] args) {
		
		try {
			Utility.configure();
			
			byte[] data = new byte[1024];
			int bytes = 0;
			
			FileInputStream inStream = new FileInputStream(new File("input01.txt"));
			JZFSFileOutputStream outputStream = new JZFSFileOutputStream("JZFS/input/input01.txt");
			
			while ((bytes = inStream.read(data)) != -1) {
				outputStream.write(data, 0, bytes);
			}
			
			inStream.close();
			outputStream.close();
			
			System.out.println("Finish adding files.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

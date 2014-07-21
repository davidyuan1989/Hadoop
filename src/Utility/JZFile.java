package Utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.NoSuchFileException;

import JZFS.JZFSClient;
import JZFS.JZFSFileInputStream;
import JZFS.JZFSFileOutputStream;

public class JZFile implements Serializable {

	private static final long serialVersionUID = 9L;
	
	public static final int JZFileSystem = 0;
	public static final int LocalFileSystem = 1;

	public int fileSystem = LocalFileSystem;
	public String path;
	public long size;

	public JZFile(int fileSystem, String path, long size) {
		this.fileSystem = fileSystem;
		this.path = path;
		this.size = size;
	}

	public InputStream getInputStream() {
		try {
			if (fileSystem == JZFile.JZFileSystem) {
				return new JZFSFileInputStream(path);
			}
			else if (fileSystem == JZFile.LocalFileSystem) {
				return new FileInputStream(new File(path));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchFileException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public OutputStream getOutputStream() {
		try {
			if (fileSystem == JZFile.JZFileSystem) {
				return new JZFSFileOutputStream(path);
			}
			else if (fileSystem == JZFile.LocalFileSystem) {
				return new FileOutputStream(new File(path));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	public static boolean checkExist(int fileSystem, String pathName) {

		if (fileSystem == JZFile.JZFileSystem) {
			return new JZFSClient().checkExist(pathName);
		}

		else if (fileSystem == JZFile.LocalFileSystem) {
			return new File(pathName).exists();
		}

		else {
			System.out.println("File system specified does not exist!");
			return false;
		}
	}
}

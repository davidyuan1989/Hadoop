package Utility;

import java.io.Serializable;
import java.util.Comparator;

public class StringComparator implements Comparator<String>, Serializable{

	private static final long serialVersionUID = 13L;

	@Override
	public int compare(String o1, String o2) {
		int ret = o1.compareTo(o2);
		if (ret > 0) {
			return 1;
		}
		else if (ret < 0) {
			return -1;
		}
		else {
			return 0;
		}
	}
}

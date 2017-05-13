package part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private Text element; 
	private Text neighbour;

	public Pair() {
		element = new Text();
		neighbour = new Text();
	}

	public Pair(String firstStringElement, String secondStringElement) {
		element = new Text(firstStringElement);
		neighbour = new Text(secondStringElement);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result	+ ((element == null) ? 0 : element.hashCode());
		result = prime * result + ((neighbour == null) ? 0 : neighbour.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		Pair other = (Pair) obj;
		if (element == null) {
			if (other.element != null)
				return false;
		}else if (!element.equals(other.element))
			return false;
		
		if (neighbour == null) {
			if (other.neighbour != null)
				return false;
		} else if (!neighbour.equals(other.neighbour))
			return false;
		
		return true;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		element.readFields(input);
		neighbour.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		element.write(output);
		neighbour.write(output);
	}

	@Override
	public int compareTo(Pair p) {
		int compare = element.compareTo(p.element);
		if (compare != 0)
			return compare;
		if (neighbour.toString() == "*")
			return -1;
		else if (p.neighbour.toString() == "*")
			return 1;
		else
			return neighbour.compareTo(p.neighbour);
	}

	@Override
	public String toString() {
		return "(" + element + ", " + neighbour + ")";
	}

	public Text getElement() {
		return element;
	}

	public Text getNeighbour() {
		return neighbour;
	}

}
package org.apache.hadoop.examples;


public class Location {
	private int x;
	private int y;
	public Location(int x, int y) {
		this.x = x;
		this.y = y;
	}

	@Override
	public int hashCode() {
		return x*12387 + y;
	}

	@Override
	public boolean equals(Object o) {
		if(o == null) return false;
		if(o instanceof Location) {
			Location other = (Location) o;
			return x == other.x && y == other.y;
		}
		return false;
	}
}

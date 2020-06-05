package de.fraunhofer.iais.kd.bda.spark;


import java.util.Set;
import java.io.Serializable;
import java.util.HashSet;


public class Userset implements Serializable {
	public Set<String> userset = new HashSet<String>(); 
	
	//add a user to the userset
	public Userset add (String username) {
		userset.add(username);
		return this;
	}
	
	//add a userset to the userset
	public Userset add (Userset set) {
		userset.addAll(set.userset);
		return this;
	}
	
	public double distanceTo(Userset other) {
		Set<String> intersection = new HashSet<String>(userset);
		intersection.retainAll(other.userset);
		
		Set<String> union = new HashSet<String>(userset);
		union.addAll(other.userset);
		
		return 1.0 - ((double) intersection.size()/union.size());
	}
	
	public String toString() {
		return userset.toString();
	}
	
	public static void main(String[] args) {
		Userset a = new Userset();
		a.add("u1");
		a.add("u2");
		
		Userset b = new Userset();
		b.add("u1");
		b.add("u3");
		
		System.out.println(a.distanceTo(b));
	}
}

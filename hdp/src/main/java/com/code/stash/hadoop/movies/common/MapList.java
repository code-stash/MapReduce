package com.code.stash.hadoop.movies.common;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public  class MapList extends AbstractList<MapInput>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private List<MapInput> unmodifiableInputs;
	
	public MapList(List<MapInput> inputs){
		unmodifiableInputs = Collections.unmodifiableList(inputs);
	}

	@Override
	public MapInput get(int index) {
		return unmodifiableInputs.get(index);
	}

	@Override
	public int size() {
		return unmodifiableInputs.size();
	}
	
	public boolean requiresMultiInput(){
		return unmodifiableInputs.size() > 1;
	}
	

	

}

package com.ericsson.dm.transform.implementation;

import java.util.Map;
import java.util.Set;

public class CommonFunctions {
	Set<String> onlyLog;
	
	public CommonFunctions() {
		// TODO Auto-generated constructor stub
	}
	
	public <K, V> K getKey(Map<K, V> map, V value) {
		return map.keySet()
						.stream()
						.filter(key -> value.equals(map.get(key)))
						.findFirst().get();
	}

}

package com.zitlab.filemgmt.store;

public class OwnerInfo {
	public static final char SEPARATOR = ':';
	private String ciType;
	private Integer citId;
	private Long ciId;
	public OwnerInfo(String ownerKey) {
		String[] keys = ownerKey.split(":");
		if(3 == keys.length) {
			ciType = keys[0];
			citId = Integer.parseInt(keys[1]);
			ciId = Long.parseLong(keys[2]);
		}
	}
	
	public OwnerInfo(String ciType, Integer citId, Long ciId) {
		this.ciType = ciType;
		this.citId = citId;
		this.ciId = ciId;
	}
	
	public String toString() {
		return concat(SEPARATOR, ciType, citId, ciId);
	}
	
	public String concat(char separator, Object... args) {
		StringBuilder sb = new StringBuilder(100);
		for(Object arg: args) {
			sb.append(arg);
			sb.append(separator);
		}
		return sb.substring(0, sb.length()-1);
	}

	public String getDefaultFilePath(String filename) {
		return concat('/', ciType, ciId, filename);
	}
	
	public String getCiType() {
		return ciType;
	}

	public Integer getCitId() {
		return citId;
	}

	public Long getCiId() {
		return ciId;
	}
}

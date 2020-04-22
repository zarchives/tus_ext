package com.zitlab.filemgmt.service;

import me.desair.tus.server.exception.UploadNotFoundException;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadInfo;

public interface IndexStorage {
	public UploadInfo create(UploadInfo info, String fileName);
	
	public UploadInfo update(UploadInfo info) throws UploadNotFoundException;
	
	public UploadInfo get(UploadInfo info) throws UploadNotFoundException;
	
	public UploadInfo getByFilePath(String filePath) throws UploadNotFoundException;
	
	public UploadInfo get(UploadId id) throws UploadNotFoundException;
	
	public UploadInfo get(long id) throws UploadNotFoundException;
	
	public int delete(UploadId id);
	
	public String getFilePath(UploadInfo info) throws UploadNotFoundException;
	
	public String newFilePath(UploadInfo info);
	//public void setFilePath(String filePath);
}

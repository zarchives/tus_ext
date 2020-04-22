package com.zitlab.filemgmt.service;

import me.desair.tus.server.upload.UploadInfo;

public interface FluwizFileHandler {
	public void preHandle(UploadInfo info);
	
	public void postHandle(UploadInfo info);
	
	public String getSubDirectory(UploadInfo info);
}

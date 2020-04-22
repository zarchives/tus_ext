package com.zitlab.filemgmt.store;

import java.util.Map;

import com.zitlab.ddm.core.datamgr.script.FrameworkHelper;

import me.desair.tus.server.upload.UploadInfo;

public class AttachmentHandler {
	//Returns the relative filePath as calculated
	public String generateFilePath(FrameworkHelper helper, UploadInfo info) {
		OwnerInfo owner = new OwnerInfo(info.getOwnerKey());
		return owner.getDefaultFilePath(info.getFileName());
	}
	
	public String generateFilePath(FrameworkHelper helper, Map<String, String> params, OwnerInfo owner, String filename) {
		return owner.getDefaultFilePath(filename);
	}
	
	// executes the complete action and returns the updated relative filePath if any
	// otherwise return the original fileName.
	public void onComplete(FrameworkHelper helper, UploadInfo info) {
		
	}
}

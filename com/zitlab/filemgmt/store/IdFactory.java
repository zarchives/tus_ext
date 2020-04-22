package com.zitlab.filemgmt.store;

import java.io.Serializable;

import me.desair.tus.server.upload.UUIDUploadIdFactory;
import me.desair.tus.server.upload.UploadId;

public class IdFactory extends UUIDUploadIdFactory{
	public UploadId getUploadId(String id) {
		Serializable idValue = getIdValueIfValid(id);
		if(null != idValue)
			return new UploadId(idValue);
		else
			return null;
	}
	
}
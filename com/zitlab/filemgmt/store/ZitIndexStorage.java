package com.zitlab.filemgmt.store;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zitlab.ddm.core.datamgr.CfgItemService;
import com.zitlab.ddm.core.pojo.CfgItem;
import com.zitlab.ddm.core.pojo.CfgItemFilter;
import com.zitlab.filemgmt.service.IndexStorage;
import com.zitlab.sql2js.base.CITypes;
import com.zitlab.sql2js.ddbc.query.helper.AddlFilter;

import me.desair.tus.server.exception.UploadNotFoundException;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadInfo;
import me.desair.tus.server.upload.UploadType;

public class ZitIndexStorage implements IndexStorage {
	private CfgItemService cfgItemService;
	public static final String OWNERKEY_SEPARATOR = ":";
	private GroovyHandler fileHandler = null;
	private static final Logger logger = LoggerFactory.getLogger(ZitIndexStorage.class);

	@Override
	public UploadInfo create(UploadInfo info, String filePath) {
		OwnerInfo ownerInfo = new OwnerInfo(info.getOwnerKey());
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		
		// set the attributes only if the new values are non-empty
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_CITID, ownerInfo.getCitId());
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_CIID, ownerInfo.getCiId());
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_FILENAME, info.getFileName());
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_FILETYPE, info.getFileMimeType());
//		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_METADATA, info.getEncodedMetadata());
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_FID, info.getId().toString());
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_PATH, filePath);
		if (info.isUploadInProgress())
			item.setAttribute(ZitAttachment.FIELD_EXPIRATION, info.getExpirationTimestamp());
		// Always consider the offset from the file
		item.setAttribute(ZitAttachment.FIELD_OFFSET, info.getOffset());
		Float percent = 100 * Float.valueOf(info.getOffset()) / info.getLength();
		item.setAttribute(ZitAttachment.FIELD_STATUS, percent);
		item.setAttribute(ZitAttachment.FIELD_SIZE, info.getLength());
		cfgItemService.saveItem(item);
		return info;
	}

	@Override
	public UploadInfo update(UploadInfo info) throws UploadNotFoundException{
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		CfgItem dbItem = getItem(info.getId());
		if(null != dbItem)
			item.setId(dbItem.getId());
	
		// set the attributes only if the new values are non-empty
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_FILETYPE, info.getFileMimeType());
//		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_METADATA, info.getEncodedMetadata());
		
		// Always consider the offset from the file
		item.setAttribute(ZitAttachment.FIELD_OFFSET, info.getOffset());
		Float percent = 100 * Float.valueOf(info.getOffset()) / info.getLength();
		item.setAttribute(ZitAttachment.FIELD_STATUS, percent);
		item.setAttribute(ZitAttachment.FIELD_SIZE, info.getLength());
		if (info.isUploadInProgress())
			item.setAttribute(ZitAttachment.FIELD_EXPIRATION, info.getExpirationTimestamp());
		else
			item.setAttribute(ZitAttachment.FIELD_EXPIRATION, null);
		cfgItemService.saveItem(item);
		if(info.getOffset() == info.getLength()) {
			fileHandler.onComplete(info);
		}
		return info;

	}

	@Override
	public UploadInfo get(UploadId id) throws UploadNotFoundException {
		CfgItem item = getItem(id);
		return setItem(item);
		
	}

	@Override
	public UploadInfo get(long id) throws UploadNotFoundException {
		CfgItem item = cfgItemService.getItem(CITypes.ATTACHMENT, id);
		if (null != item)
			return setItem(item);
		else
			return null;
	}

	private UploadInfo setItem(CfgItem item) {
		UploadInfo info = new UploadInfo();
		Integer citId = item.getAttributeAsInt(ZitAttachment.FIELD_CITID);
		Long ciId = item.getAttributeAsLong(ZitAttachment.FIELD_CIID);
		UploadId uploadId = new IdFactory().getUploadId(item.getAttributeAsString(ZitAttachment.FIELD_FID));
		String ciType = item.getAttributeAsString(ZitAttachment.FIELD_CITYPE);
		info.setId(uploadId);
		info.setUploadType(UploadType.REGULAR);
		//info.setEncodedMetadata(item.getAttributeAsString(ZitAttachment.FIELD_METADATA));
		info.setLength(item.getAttributeAsLong(ZitAttachment.FIELD_SIZE));
		info.setOffset(item.getAttributeAsLong(ZitAttachment.FIELD_OFFSET));
		OwnerInfo ownerInfo = new OwnerInfo(ciType, citId, ciId);
		info.setOwnerKey(ownerInfo.toString());
		Long expiration = item.getAttributeAsLong(ZitAttachment.FIELD_EXPIRATION);
		if (null != expiration) {
			Long curTime = new Date().getTime();
			info.updateExpiration(expiration - curTime);
		}
		
		return info;
	}

	@Override
	public int delete(UploadId id) {
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_FID, id.toString());
		return cfgItemService.deleteItem(item);
	}

	@Override
	public String getFilePath(UploadInfo info) throws UploadNotFoundException{
		CfgItem item = getItem(info.getId());
		if(null != item)
			return item.getAttributeAsString(ZitAttachment.FIELD_PATH);
		
		return null;
	}
	
	private CfgItem getItem(UploadId id) throws UploadNotFoundException {
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);		
		if(null == id)
			throw new UploadNotFoundException("Null Id received while retriting Upload information");
		item.setAttribute(ZitAttachment.FIELD_FID, id.toString());
		CfgItemFilter filter = new CfgItemFilter();
		filter.setFilter(item);
		List<CfgItem> items = cfgItemService.searchItem(filter, new AddlFilter());
		if (items.size() == 1) {
			CfgItem dbItem = items.get(0);
			return dbItem;
		}
		else if (items.size() > 1)
			logger.error("More than one entry found in the attachment. Please check the constraints");

		throw new UploadNotFoundException("Attachment with id " + id + " not found in the system");
	}
	
	@Override
	public String newFilePath(UploadInfo info) {
		String filePath = fileHandler.generateFilePath(info);
		logger.debug("Generated filepath is {}, for the name {}, ownerkey {}", filePath, info.getFileName(), info.getOwnerKey());
		return filePath;
	}

	public CfgItemService getCfgItemService() {
		return cfgItemService;
	}

	public void setCfgItemService(CfgItemService cfgItemService) {
		this.cfgItemService = cfgItemService;
	}

	@Override
	public UploadInfo get(UploadInfo info) {
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		if (null != info.getId())
			item.setAttribute(ZitAttachment.FIELD_FID, info.getId().toString());
		CfgItemFilter filter = new CfgItemFilter();
		filter.setFilter(item);
		List<CfgItem> items = cfgItemService.searchItem(filter, new AddlFilter());
		if (items.size() == 1)
			return setItem(items.get(0));
		else if (items.size() > 1)
			logger.error("More than one entry found in the attachment. Please check the constraints");

		return null;
	}

	public UploadInfo getByFilePath(String filePath) {
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		item.setAttribute(ZitAttachment.FIELD_PATH, filePath);
		CfgItemFilter filter = new CfgItemFilter();
		filter.setFilter(item);
		List<CfgItem> items = cfgItemService.searchItem(filter, new AddlFilter());
		if (items.size() == 1)
			return setItem(items.get(0));
		else if (items.size() > 1)
			logger.error("More than one entry found in the attachment. Please check the constraints");
		return null;
	}

	public GroovyHandler getFileHandler() {
		return fileHandler;
	}

	public void setFileHandler(GroovyHandler fileHandler) {
		this.fileHandler = fileHandler;
	}
	
}

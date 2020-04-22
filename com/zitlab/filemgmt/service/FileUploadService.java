package com.zitlab.filemgmt.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import com.zitlab.ddm.core.datamgr.CfgItemService;
import com.zitlab.ddm.core.pojo.CfgItem;
import com.zitlab.ddm.core.pojo.CfgItemFilter;
import com.zitlab.filemgmt.store.AbstractDatabaseBasedStorageService;
import com.zitlab.filemgmt.store.GroovyHandler;
import com.zitlab.filemgmt.store.OwnerInfo;
import com.zitlab.filemgmt.store.ZitAttachment;
import com.zitlab.sql2js.base.CITypes;
import com.zitlab.sql2js.ddbc.query.helper.AddlFilter;

import me.desair.tus.server.upload.UploadInfo;

public class FileUploadService extends AbstractDatabaseBasedStorageService {

	private CfgItemService cfgItemService;
	private GroovyHandler fileHandler;
	private static final Logger logger = LoggerFactory.getLogger(FileUploadService.class);
	private final Tika tika = new Tika();
	
	public FileUploadService(String path) {
		super(path);
	}

	@Transactional
	public void process(HttpServletRequest request, HttpServletResponse response, OwnerInfo ownerInfo,
			Map<String, String> params) throws IOException {
		UploadInfo info = new UploadInfo(request);
		info.setLength(request.getContentLengthLong());
		info.setOwnerKey(ownerInfo.toString());
		String type = request.getContentType();
		String filename = params.get("fileName");

		String endURL = fileHandler.generateFilePath(params, ownerInfo, filename);
		Path filePath = getBytesPath(endURL);
		if(null != getByFile(endURL) || filePath.toFile().exists()) {
			throw new FileAlreadyExistsException("File already exists " + endURL);
		}

		if (!filePath.getParent().toFile().exists()) {
			createSubDirectory(filePath);
		}

		InputStream in = request.getInputStream();
		Files.copy(in, filePath);
		try {
			createRecord(ownerInfo, filename, endURL, type);
		} catch (Throwable e) {
			Files.delete(filePath);
			throw e;
		}
	}

	private void createRecord(OwnerInfo ownerInfo, String filename, String filePath, String filetype) {
		// Save the Record to the database
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		Path path = getBytesPath(filePath);
		File file = path.toFile();
		long length = file.length();
		item.setAttribute(ZitAttachment.FIELD_CITID, ownerInfo.getCitId());
		item.setAttribute(ZitAttachment.FIELD_CIID, ownerInfo.getCiId());
		item.setAttribute(ZitAttachment.FIELD_FILENAME, filename);
		item.setAttributeIfNotEmpty(ZitAttachment.FIELD_FILETYPE, getFileType(path));
		item.setAttribute(ZitAttachment.FIELD_FID, generateId());
		item.setAttribute(ZitAttachment.FIELD_PATH, filePath);

		item.setAttribute(ZitAttachment.FIELD_OFFSET, length);
		item.setAttribute(ZitAttachment.FIELD_STATUS, 100);
		item.setAttribute(ZitAttachment.FIELD_SIZE, length);
		cfgItemService.saveItem(item);
	}
	
	private String generateId() {
		String fid;		
		do{
			fid = UUID.randomUUID().toString();
		}while(null != get(fid));
		return fid;
	}
	
	private String getFileType(Path path) {
		try {
			return tika.detect(path);
		}catch(IOException e) {
			return "application/octet-stream";
		}
	}
	
	private CfgItem get(String fid) {
		// Get the record from the database by Id
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		item.setAttribute(ZitAttachment.FIELD_FID, fid);
		CfgItemFilter filter = new CfgItemFilter();
		filter.setFilter(item);
		List<CfgItem> items = cfgItemService.searchItem(filter, new AddlFilter());
		int length = items.size();
		if(0 == length)
			return null;
		if(length > 1)
			logger.error("Multiple references found for the attachment fid " + fid);
		
		return items.get(0);
	}
	
	public CfgItem getByFile(String filePath) {
		// Get the record by filename
		CfgItem item = new CfgItem(CITypes.ATTACHMENT);
		item.setAttribute(ZitAttachment.FIELD_PATH, filePath);
		CfgItemFilter filter = new CfgItemFilter();
		filter.setFilter(item);
		List<CfgItem> items = cfgItemService.searchItem(filter, new AddlFilter());
		int length = items.size();
		if(0 == length)
			return null;
		if(length > 1)
			logger.error("Multiple references found for the filePath " + filePath);
		
		return items.get(0);
	}

	public void setCfgItemService(CfgItemService cfgItemService) {
		this.cfgItemService = cfgItemService;
	}
	
	public void setFileHandler(GroovyHandler fileHandler) {
		this.fileHandler = fileHandler;
	}
}

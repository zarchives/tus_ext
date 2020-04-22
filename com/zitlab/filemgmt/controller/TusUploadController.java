package com.zitlab.filemgmt.controller;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

import javax.annotation.PostConstruct;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.zitlab.ddm.core.datamgr.CfgItemService;
import com.zitlab.ddm.core.datamgr.utils.AppConfig;
import com.zitlab.ddm.exception.DDMException;
import com.zitlab.filemgmt.service.TusUploadService;
import com.zitlab.filemgmt.store.DataDiskStorageService;
import com.zitlab.filemgmt.store.GroovyHandler;
import com.zitlab.filemgmt.store.IdFactory;
import com.zitlab.filemgmt.store.OwnerInfo;
import com.zitlab.filemgmt.store.ZitIndexStorage;
import com.zitlab.sql2js.base.CfgItemType;
import com.zitlab.sql2js.base.Schema;
import com.zitlab.sql2js.base.SchemaCfg;
import com.zitlab.uiweb.layout.widget.Page;
import com.zitlab.uiweb.service.ExplorerPageService;

import me.desair.tus.server.TusFileUploadService;
import me.desair.tus.server.upload.UploadLockingService;
import me.desair.tus.server.upload.disk.DiskLockingService;

@Controller
@RequestMapping({ "/v{version}/{customer}/tusupload" })
public class TusUploadController {

	private static final Logger logger = LoggerFactory.getLogger(TusUploadController.class);
	private static final String URL_PATTERN = "/.*/tusupload/[a-z_A-Z]*/[0-9]+/?";
	private static String uploadFolder;
	private static Long expirationPeriod;
	private ExplorerPageService pageService;
	private CfgItemService cfgService;
	private DataDiskStorageService storageService;
	private TusFileUploadService uploadService;
	private UploadLockingService lockingService; 
	private GroovyHandler fileHandler;

	@PostConstruct
	public void init() {
		logger.debug("Initializing Upload service with startup parameters");
		try {
			uploadFolder = AppConfig.get("upload.folder");

			expirationPeriod = AppConfig.getLong("tus.expiration.interval");

			if (null == expirationPeriod)
				expirationPeriod = 7 * 24 * 60 * 60 * 1000L; // Set for 7 days in milliseconds
			if (null == uploadFolder)
				uploadFolder = "/tmp/upload";

			lockingService = new DiskLockingService(uploadFolder);
			storageService = new DataDiskStorageService(uploadFolder);
			fileHandler = new GroovyHandler();
			fileHandler.setCmdbService(cfgService);
			
			ZitIndexStorage indexStorage = new ZitIndexStorage();
			indexStorage.setCfgItemService(cfgService);
			indexStorage.setFileHandler(fileHandler);
			storageService.setIndexStorage(indexStorage);
			
			uploadService = new TusUploadService().withUploadURI(URL_PATTERN).withStoragePath(uploadFolder)
					.withUploadStorageService(storageService).withUploadExpirationPeriod(expirationPeriod)
					.withUploadLockingService(lockingService)
					.withUploadIdFactory(new IdFactory());
			
			logger.info("Files will be uploaded to " + uploadFolder);
		} catch (Exception e) {
			logger.error("", e);
			throw new DDMException("Error while initializing ", e);
		}
	}

	@RequestMapping({ "/{page}/{ciId}","/{page}/{ciId}/"  })
	public void newUpload(HttpServletRequest request, HttpServletResponse response, @PathVariable("page") String page,
			@PathVariable(name = "ciId") Long ciId) throws IOException, ServletException {
		try {
			service(request, response, page, ciId, null);
		} catch (FileAlreadyExistsException e) {
			response.setStatus(HttpStatus.FOUND.value());
		}
	}

	@RequestMapping({ "/{page}/{ciId}/{fileId}" })
	public void service(HttpServletRequest request, HttpServletResponse response, @PathVariable("page") String page,
			@PathVariable(name = "ciId") Long ciId, @PathVariable(name = "fileId", required = true) String fileId)
			throws IOException, ServletException {
		// Internal processing -- removed
	}

	@Autowired
	public void setPageService(ExplorerPageService pageService) {
		this.pageService = pageService;
	}

	@Autowired
	public void setCfgService(CfgItemService cfgService) {
		this.cfgService = cfgService;
	}
}

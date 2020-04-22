package com.zitlab.filemgmt.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.desair.tus.server.TusFileUploadService;
import me.desair.tus.server.upload.disk.StoragePathNotAvailableException;

/**
 * Common abstract super class to implement service that use the disk file
 * system
 */
public class AbstractDatabaseBasedStorageService {
	private static final String UPLOAD_SUB_DIRECTORY = "data";
	private static final Logger log = LoggerFactory.getLogger(TusFileUploadService.class);

	private Path storagePath;

	public AbstractDatabaseBasedStorageService(String path) {
		Validate.notBlank(path, "The storage path cannot be blank");
		this.storagePath = Paths.get(path + File.separator + UPLOAD_SUB_DIRECTORY);
		if (!Files.exists(storagePath)) {
			init();
		}
	}

	protected Path getStoragePath() {
		return storagePath;
	}

	protected Path createSubDirectory(Path filePath) throws IOException {
		return createDirectories(filePath.getParent());
	}

	protected Path createSubDirectory(String filePath) throws IOException {
		return createDirectories(getStoragePath().resolve(filePath).getParent());
	}

	private Path createDirectories(Path dir) throws IOException {
		if (!dir.toFile().exists())
			return Files.createDirectories(dir);
		return dir;
	}

	protected Path getBytesPath(String filePath) {
		Path file = storagePath.resolve(filePath);
//		if (Files.exists(file.getParent())) {
		return file.resolve(file);
//		} else
//			throw new RuntimeException("Parent Directory " + file.toString() + " not found");
	}

	private synchronized void init() {
		if (!Files.exists(storagePath)) {
			try {
				Files.createDirectories(storagePath);
			} catch (IOException e) {
				String message = "Unable to create the directory specified by the storage path " + storagePath;
				log.error(message, e);
				throw new StoragePathNotAvailableException(message, e);
			}
		}
	}
}

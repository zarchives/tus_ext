package com.zitlab.filemgmt.store;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zitlab.filemgmt.service.IndexStorage;

import me.desair.tus.server.exception.InvalidUploadOffsetException;
import me.desair.tus.server.exception.TusException;
import me.desair.tus.server.exception.UploadNotFoundException;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadIdFactory;
import me.desair.tus.server.upload.UploadInfo;
import me.desair.tus.server.upload.UploadLockingService;
import me.desair.tus.server.upload.UploadStorageService;
import me.desair.tus.server.upload.UploadType;
import me.desair.tus.server.upload.concatenation.UploadConcatenationService;
import me.desair.tus.server.upload.concatenation.VirtualConcatenationService;

/**
 * Implementation of {@link UploadStorageService} that implements storage on
 * disk
 */
public class DataDiskStorageService extends AbstractDatabaseBasedStorageService implements UploadStorageService {

	private static final Logger logger = LoggerFactory.getLogger(DataDiskStorageService.class);

	private Long maxUploadSize = null;
	// set expiration Period of 5 Seconds
	private Long uploadExpirationPeriod = null;
	private UploadIdFactory idFactory;
	private UploadConcatenationService uploadConcatenationService;
	private IndexStorage indexStorage = null;

	public DataDiskStorageService(String storagePath) {
		super(storagePath);
		setUploadConcatenationService(new VirtualConcatenationService(this));
	}

	public DataDiskStorageService(UploadIdFactory idFactory, String storagePath) {
		this(storagePath);
		Validate.notNull(idFactory, "The IdFactory cannot be null");
		this.idFactory = idFactory;
	}

	@Override
	public void setIdFactory(UploadIdFactory idFactory) {
		Validate.notNull(idFactory, "The IdFactory cannot be null");
		this.idFactory = idFactory;
	}

	@Override
	public void setMaxUploadSize(Long maxUploadSize) {
		this.maxUploadSize = (maxUploadSize != null && maxUploadSize > 0 ? maxUploadSize : 0);
	}

	@Override
	public long getMaxUploadSize() {
		return maxUploadSize == null ? 0 : maxUploadSize;
	}

	@Override
	public UploadInfo getUploadInfo(String uploadUrl, String ownerKey) throws IOException {
		logger.debug("retrieving the uploadId from the URL {}", uploadUrl);
		UploadInfo uploadInfo = getUploadInfo(idFactory.readUploadId(uploadUrl));
		if (uploadInfo == null || !Objects.equals(uploadInfo.getOwnerKey(), ownerKey)) {
			return null;
		} else {
			return uploadInfo;
		}
	}

	/**
	 * Retrieve the Upload Information
	 */
	@Override
	public UploadInfo getUploadInfo(UploadId id) throws IOException {
		logger.debug("Getting upload information for the uploadId {}", id);
		try {
			UploadInfo info = indexStorage.get(id);
			if (null != info) {
				if (logger.isTraceEnabled())
					logger.trace("Upload informatoin found {}", id);
				Path path = getBytesPath(indexStorage.getFilePath(info));
				if (null != path) {
					File file = path.toFile();
					if (file.exists())
						info.setOffset(file.length());
				}
			}
			return info;
		} catch (UploadNotFoundException e) {
			return null;
		}
	}

	public Path getFilePathforDownload(UploadId id) {
		UploadInfo info;
		try {
			info = indexStorage.get(id);
			if (!info.isUploadInProgress())
				return getBytesPath(indexStorage.getFilePath(info));
			else
				logger.error("Cannot download the file while upload is in progress {}", id);
		} catch (UploadNotFoundException e) {
		}

		return null;
	}

	@Override
	public String getUploadURI() {
		return idFactory.getUploadURI();
	}

	@Override
	public UploadInfo create(UploadInfo info, String ownerKey) throws IOException {
		info.setOffset(0L);
		info.setOwnerKey(ownerKey);
		String filePath = indexStorage.newFilePath(info);
		UploadInfo dbInfo = null;

		try {
			dbInfo = indexStorage.getByFilePath(filePath);
		} catch (UploadNotFoundException e1) {
		}

		Path bytesPath = getBytesPath(filePath);

		if (null != dbInfo) {
			info.setId(dbInfo.getId());
			if (Files.exists(bytesPath)) {
				long offset = bytesPath.toFile().length();
				if (offset < dbInfo.getLength()) {
					info.setOffset(offset);
					logger.debug("Resuming incomplete upload for the file {}, {}", filePath, dbInfo.getId());
					return info;
				}
				logger.error(
						"File {} - {} already available and completely uploaded. rejecting the current upload request",
						filePath, dbInfo.getId());
				throw new FileAlreadyExistsException(filePath + " is already present in the system");
			}
		} else {
			if (Files.exists(bytesPath)) {
				logger.error("File {} already available but related info storage is missing", filePath);
				throw new FileAlreadyExistsException(filePath + " and info Storage record is missing");
			}
			UploadId id = createNewId();
			info.setId(id);
		}
		createSubDirectory(filePath);

		try {
			Files.createFile(bytesPath);
			if (null == dbInfo)
				createRecord(info, filePath);
			else
				update(info);
			return info;
		} catch (UploadNotFoundException e) {
			// Normally this cannot happen
			logger.error("Unable to create UploadInfo because of an upload not found exception", e);
		} catch (Exception ex) {
			Files.delete(bytesPath);
			throw ex;
		}
		return null;
	}

	@Override
	public void update(UploadInfo uploadInfo) throws IOException, UploadNotFoundException {

		if (null != uploadExpirationPeriod && uploadInfo.isUploadInProgress())
			uploadInfo.updateExpiration(uploadExpirationPeriod);
		indexStorage.update(uploadInfo);
	}

	private void createRecord(UploadInfo uploadInfo, String filePath) throws IOException, UploadNotFoundException {
		indexStorage.create(uploadInfo, filePath);
	}

	private Path getBytesPath(UploadInfo info) throws IOException, UploadNotFoundException {
		String filePath = indexStorage.getFilePath(info);
		return getBytesPath(filePath);
	}

	@Override
	public UploadInfo append(UploadInfo info, InputStream inputStream) throws IOException, TusException {
		if (info != null) {
			Path bytesPath = getBytesPath(info);

			long max = getMaxUploadSize() > 0 ? getMaxUploadSize() : Long.MAX_VALUE;
			long transferred = 0;
			Long offset = info.getOffset();
			long newOffset = offset;

			try (ReadableByteChannel uploadedBytes = Channels.newChannel(inputStream);
					FileChannel file = FileChannel.open(bytesPath, WRITE)) {

				try {
					// Lock will be released when the channel closes
					file.lock();

					// Validate that the given offset is at the end of the file
					if (!offset.equals(file.size())) {
						throw new InvalidUploadOffsetException("The upload offset does not correspond to the written"
								+ " bytes. You can only append to the end of an upload");
					}

					// write all bytes in the channel up to the configured maximum
					transferred = file.transferFrom(uploadedBytes, offset, max - offset);
					file.force(true);
					newOffset = offset + transferred;

				} catch (Exception ex) {
					// An error occurred, try to write as much data as possible
					newOffset = writeAsMuchAsPossible(file);
					throw ex;
				}

			} finally {
				info.setOffset(newOffset);
				update(info);
			}
		}

		return info;
	}

	@Override
	public void removeLastNumberOfBytes(UploadInfo info, long byteCount) throws UploadNotFoundException, IOException {

		if (info != null && byteCount > 0) {
			Path bytesPath = getBytesPath(info);

			try (FileChannel file = FileChannel.open(bytesPath, WRITE)) {

				// Lock will be released when the channel closes
				file.lock();

				file.truncate(file.size() - byteCount);
				file.force(true);

				info.setOffset(file.size());
				update(info);
			}
		}
	}

	@Override
	public void terminateUpload(UploadInfo info) throws UploadNotFoundException, IOException {
		if (info != null) {
			File file = getBytesPath(info).toFile();
			if (file.exists())
				file.delete();
			indexStorage.delete(info.getId());
		}
	}

	@Override
	public Long getUploadExpirationPeriod() {
		return uploadExpirationPeriod;
	}

	@Override
	public void setUploadExpirationPeriod(Long uploadExpirationPeriod) {
		this.uploadExpirationPeriod = uploadExpirationPeriod;
	}

	@Override
	public void setUploadConcatenationService(UploadConcatenationService concatenationService) {
		Validate.notNull(concatenationService);
		this.uploadConcatenationService = concatenationService;
	}

	@Override
	public UploadConcatenationService getUploadConcatenationService() {
		return uploadConcatenationService;
	}

	@Override
	public InputStream getUploadedBytes(String uploadURI, String ownerKey) throws IOException, UploadNotFoundException {

		UploadId id = idFactory.readUploadId(uploadURI);

		UploadInfo uploadInfo = getUploadInfo(id);
		if (uploadInfo == null || !Objects.equals(uploadInfo.getOwnerKey(), ownerKey)) {
			throw new UploadNotFoundException("The upload with id " + id + " could not be found for owner " + ownerKey);
		} else {
			return getUploadedBytes(id);
		}
	}

	@Override
	public InputStream getUploadedBytes(UploadId id) throws IOException, UploadNotFoundException {
		if (1 == 1)
			throw new RuntimeException("this method is not verified");
		InputStream inputStream = null;
		UploadInfo uploadInfo = getUploadInfo(id);
		if (UploadType.CONCATENATED.equals(uploadInfo.getUploadType()) && uploadConcatenationService != null) {
			inputStream = uploadConcatenationService.getConcatenatedBytes(uploadInfo);

		} else {
			Path bytesPath = getBytesPath(uploadInfo);
			// If bytesPath is not null, we know this is a valid Upload URI
			if (bytesPath != null) {
				inputStream = Channels.newInputStream(FileChannel.open(bytesPath, READ));
			}
		}

		return inputStream;
	}

	@Override
	public void copyUploadTo(UploadInfo info, OutputStream outputStream) throws UploadNotFoundException, IOException {

		List<UploadInfo> uploads = getUploads(info);

		WritableByteChannel outputChannel = Channels.newChannel(outputStream);

		for (UploadInfo upload : uploads) {
			if (upload == null) {
				logger.warn("We cannot copy the bytes of an upload that does not exist");

			} else if (upload.isUploadInProgress()) {
				logger.warn("We cannot copy the bytes of upload {} because it is still in progress", upload.getId());

			} else {
				Path bytesPath = getBytesPath(info);
				try (FileChannel file = FileChannel.open(bytesPath, READ)) {
					// Efficiently copy the bytes to the output stream
					file.transferTo(0, upload.getLength(), outputChannel);
				}
			}
		}
	}

	@Override
	public void cleanupExpiredUploads(UploadLockingService uploadLockingService) throws IOException {
		throw new RuntimeException("This method yet to be implemneted");
	}

	private List<UploadInfo> getUploads(UploadInfo info) throws IOException, UploadNotFoundException {
		List<UploadInfo> uploads;

		if (info != null && UploadType.CONCATENATED.equals(info.getUploadType())
				&& uploadConcatenationService != null) {
			uploadConcatenationService.merge(info);
			uploads = uploadConcatenationService.getPartialUploads(info);
		} else {
			uploads = Collections.singletonList(info);
		}
		return uploads;
	}

	private synchronized UploadId createNewId() throws IOException {
		UploadId id;
		do {
			id = idFactory.createId();
			// For extra safety, double check that this ID is not in use yet
		} while (getUploadInfo(id) != null);
		return id;
	}

	private long writeAsMuchAsPossible(FileChannel file) throws IOException {
		long offset = 0;
		if (file != null) {
			file.force(true);
			offset = file.size();
		}
		return offset;
	}

	public IndexStorage getIndexStorage() {
		return indexStorage;
	}

	public void setIndexStorage(IndexStorage indexStorage) {
		this.indexStorage = indexStorage;
	}
}

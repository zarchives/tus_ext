package com.zitlab.filemgmt.tusext;

import java.io.IOException;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import me.desair.tus.server.HttpHeader;
import me.desair.tus.server.HttpMethod;
import me.desair.tus.server.upload.UploadInfo;
import me.desair.tus.server.upload.UploadStorageService;
import me.desair.tus.server.util.AbstractRequestHandler;
import me.desair.tus.server.util.TusServletRequest;
import me.desair.tus.server.util.TusServletResponse;
import me.desair.tus.server.util.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Server MUST acknowledge a successful upload creation with the 201 Created status.
 * The Server MUST set the Location header to the URL of the created resource. This URL MAY be absolute or relative.
 * The current customization is given for returning relative Location
 */
public class ExtCreationPostRequestHandler extends AbstractRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(ExtCreationPostRequestHandler.class);

    @Override
    public boolean supports(HttpMethod method) {
        return HttpMethod.POST.equals(method);
    }

    @Override
    public void process(HttpMethod method, TusServletRequest servletRequest,
                        TusServletResponse servletResponse, UploadStorageService uploadStorageService,
                        String ownerKey) throws IOException {

        UploadInfo info = buildUploadInfo(servletRequest);
        info = uploadStorageService.create(info, ownerKey);
        
        // customization starts
        servletResponse.setHeader(HttpHeader.LOCATION, info.getId().toString());
        // customization ends
        servletResponse.setStatus(HttpServletResponse.SC_CREATED);
        if(info.getOffset() > 0) {
        	servletResponse.setHeader(HttpHeader.UPLOAD_OFFSET, Objects.toString(info.getOffset()));
        }
        log.info("Created upload with ID {} at {} for ip address {} with location {}",
                info.getId(), info.getCreationTimestamp(), info.getCreatorIpAddresses(), info.getId());
    }

    private UploadInfo buildUploadInfo(HttpServletRequest servletRequest) {
        UploadInfo info = new UploadInfo(servletRequest);

        Long length = Utils.getLongHeader(servletRequest, HttpHeader.UPLOAD_LENGTH);
        if (length != null) {
            info.setLength(length);
        }

        String metadata = Utils.getHeader(servletRequest, HttpHeader.UPLOAD_METADATA);
        if (StringUtils.isNotBlank(metadata)) {
            info.setEncodedMetadata(metadata);
        }

        return info;
    }
}

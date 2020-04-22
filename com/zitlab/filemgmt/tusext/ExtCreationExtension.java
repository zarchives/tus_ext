package com.zitlab.filemgmt.tusext;

import java.util.List;

import me.desair.tus.server.RequestHandler;
import me.desair.tus.server.RequestValidator;
import me.desair.tus.server.creation.CreationExtension;
import me.desair.tus.server.creation.CreationHeadRequestHandler;
import me.desair.tus.server.creation.CreationOptionsRequestHandler;
import me.desair.tus.server.creation.CreationPatchRequestHandler;
import me.desair.tus.server.creation.validation.PostEmptyRequestValidator;
import me.desair.tus.server.creation.validation.PostURIValidator;
import me.desair.tus.server.creation.validation.UploadDeferLengthValidator;
import me.desair.tus.server.creation.validation.UploadLengthValidator;

public class ExtCreationExtension extends CreationExtension{
	@Override
    protected void initValidators(List<RequestValidator> requestValidators) {
        requestValidators.add(new PostURIValidator());
        requestValidators.add(new PostEmptyRequestValidator());
        requestValidators.add(new UploadDeferLengthValidator());
        requestValidators.add(new UploadLengthValidator());
    }
	
	@Override
    protected void initRequestHandlers(List<RequestHandler> requestHandlers) {
        requestHandlers.add(new CreationHeadRequestHandler());
        requestHandlers.add(new CreationPatchRequestHandler());
        requestHandlers.add(new ExtCreationPostRequestHandler());
        requestHandlers.add(new CreationOptionsRequestHandler());
    }
}

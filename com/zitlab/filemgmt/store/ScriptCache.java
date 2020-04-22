package com.zitlab.filemgmt.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.codehaus.groovy.control.CompilationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zitlab.ddm.core.datamgr.utils.AppConfig;

import groovy.lang.GroovyClassLoader;

class ScriptCache {

	private static String scriptBase = AppConfig.get("groovy.script.location");
	private static String ignoreError = AppConfig.get("groovy.ignoreError");
	private static String devModeS	= AppConfig.get("groovy.devMode");
	private static boolean devMode = (null == devModeS || devModeS.equalsIgnoreCase("true")) ? true : false;
	private static HashMap<String, Class<AttachmentHandler>> classCache = new HashMap<>();

	private static boolean isCacheEnabled = !devMode;

	private static final Logger logger = LoggerFactory.getLogger(ScriptCache.class);
	
	public static boolean isCacheEnabled() {
		return isCacheEnabled;
	}

	public static void setCacheEnabled(boolean isCache) {
		isCacheEnabled = isCache;
	}

	public static void setBaseFolder(String baseFolder) {
		scriptBase = baseFolder;
	}

	public static AttachmentHandler getScript(String fileRef) {
		Class<AttachmentHandler> scriptClass = null;
		AttachmentHandler scriptObject = null;

		try {
			if (isCacheEnabled) {
				scriptClass = classCache.get(fileRef);
				if (null != scriptClass)
					return scriptClass.newInstance();
			}

			String fileLocation = getFileReference(fileRef);
			if(devMode)
				logger.trace("Loading groovy script from {}", fileRef);
			
			File file = new File(fileLocation);

			GroovyClassLoader gcl = new GroovyClassLoader(ScriptCache.class.getClassLoader());

			scriptClass = gcl.parseClass(file);
			if(devMode)
				logger.debug("Loaded class {} from the groovy script {}", scriptClass.getName(), fileRef );
				
			scriptObject = scriptClass.newInstance();
			if (isCacheEnabled)
				classCache.put(fileRef, scriptClass);

			gcl.close();
		} catch (InstantiationException e) {
			logger.error("Error while loading Script " , e);
		} catch (IllegalAccessException e) {
			logger.error("Error while loading Script " , e);
		} catch (CompilationFailedException e) {
			logger.error("Error while loading Script " , e);
		} catch (IOException e) {
			if(ignoreError.equalsIgnoreCase("false"))
				logger.error("Error while loading Script " , e);
		}

		return scriptObject;
	}

	private static String getFileReference(String fileRef) {
		StringBuilder sb = new StringBuilder(128);
		sb.append(scriptBase).append("/").append(fileRef).append(".groovy");
		return sb.toString();
	}
}
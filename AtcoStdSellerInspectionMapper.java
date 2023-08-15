package com.atc.dataservices.mapper;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.math.BigDecimal;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.atc.dataservices.adaptor.EmailAdaptor;
import com.atc.dataservices.adaptor.FileAdaptor;
import com.atc.dataservices.adaptor.SprayPhotoAdaptor;
import com.atc.dataservices.adaptor.PropertiesAdaptor;
import com.atc.dataservices.adaptor.SQLLDRAdaptor;
import com.atc.dataservices.common.AuctionUtil;
import com.atc.dataservices.common.VehicleStatusId;
import com.atc.dataservices.extraction.DirectoryReader;
import com.atc.dataservices.extraction.DirectoryUtil;
import com.atc.dataservices.extraction.XMLJavaHelper;
import com.atc.dataservices.mapper.model.AtcoStageDamage;
import com.atc.dataservices.mapper.model.AtcoStageInspection;
import com.atc.dataservices.mapper.model.AtcoStageVehicle;
import com.atc.dataservices.mapper.model.Auction;
import com.atc.dataservices.mapper.model.AuctionRelease;
import com.atc.dataservices.mapper.model.Damage;
import com.atc.dataservices.mapper.model.Picture;
import com.atc.dataservices.mapper.model.AtcoStagePicture;
import com.atc.dataservices.model.DatabaseTargetBean;
import com.atc.dataservices.model.RegistryBean;
import com.atc.dataservices.model.TaskBean;
import com.atc.dataservices.util.DateTimeUtil;
import com.atc.dataservices.vindecoder.ChromeResolveUtil;

public class AtcoStdSellerInspectionMapper extends AbstractMapper{
	
	private static Logger logger = Logger.getLogger(AtcoStdSellerInspectionMapper.class);
	private static String processTitle = "ATCO Standard Seller Inspection";
	private static String operationsRecipient = "operationsRecipient";
	private static String sellerRecipient = "standardSellerErrorRecipient";
	protected String uploadReportRecipient;
	protected Long sellerOrgId;
//	protected Long santanderSCLeaseSellerOrgId;
	protected Long inspectionOrgId;
	protected AtcoStageVehicle info;
	protected HashMap<Long, AuctionRelease> carGroupAuctionTimes = new HashMap<Long, AuctionRelease>();
	protected boolean appendStaticOrgIds = true;
	protected boolean error=true;
	
	protected StringBuffer reportBuffer;
	protected StringBuffer uploadReportBuffer = new StringBuffer();
	protected static final String REPORT_DATE_FORMAT = "MM/dd/yyyy";
	
	protected static final String TEMP_PICTURE_DIR_FORMAT = "yyyyMMddHHmmssSSS";
	
	protected byte[] byteBuffer = new byte[8 * 1024];
	protected SprayPhotoAdaptor sprayPhotoAdaptor;	
	protected List<AtcoStageVehicle> unprocessedVehicles = new ArrayList<AtcoStageVehicle>();
	protected String additionalErrorRecipient;
	protected String inspectionOrgName;
	protected ChromeResolveUtil chromeResolveUtil;
	
	protected Long countryId;
	
	public boolean batchLoad(TaskBean task, DatabaseTargetBean destination) throws Exception {		
		logger.info("Process: " + getProcessTitle());
		reportBuffer = new StringBuffer();
		boolean success;	
				
		if (identifySeller(task)) { success = true ; } else { return false; }
		if (setInspectionOrgId(task)) { success = true ; } else { return false; }
		if (deleteAllStagingTables()) { success = true ; } else { return false; }
		if (loadAllTables(task, destination)) { success = true ; } else { return false; }
		if (overwriteInfoObj(task.getAuditId())){ success = true ; } else { return false; }
		if (validateFileContent(task)) { success = true ; } else { return false; }		
		
		logger.info("Inspection Org Id is " + getInspectionOrgId());
		logger.info("Process Id is " + task.getAuditId());
		
		if (loadInspections(task)) { success = true ; } else { return false; }
		postLoadInspections(task);
		reportUnprocessedVehicles(task);

		return success;
	}
	
	protected void postLoadingInspectionData(TaskBean task){}//Called after loading inspection data but before picture upload
	protected void postLoadInspections(TaskBean task){}//After loading inspections including pictures
	protected void AdditionalBussinessProcessor(AtcoStageVehicle veh){}//After commit tables, call HCA WS
	
	protected boolean identifySeller(TaskBean task) {
		if(setSellerOrgIdByFileName(task.getSourceFileName())) return true;
		if(!setSellerOrgId(task.getTaskFile())){
			String subject = getProcessTitle() + " Upload Failed - Filename: " + task.getSourceFileName();
			String body = "None of the attached VINS are in the system (VEHICLES table). \n\n" +
				"Please verify Consignment File for all the VINS before running the inspection";
			sendErrorDocument(getOperationsRecipient(),task.getSourceFileName(), task.getSourceFile(), subject, body);
			logger.info("None of the VINS in the file exists in the sytem, no inspections loaded. Email was sent");
			return false;
		}
		return true;
	}
	
	//Refactored from loadInspections(). This one assumes that there's only 1 seller org id.
	//Overwrite it if there're more than 1 sellers
	protected List<AtcoStageVehicle> getAllLoadedVins() throws Exception{
		return (List<AtcoStageVehicle>)dataBaseServiceAdaptor.queryList("AllLoadedVins", getInfo());
	}
	
	protected boolean loadInspections(TaskBean task) throws Exception {		
		try{
			getInfo().setDbProcessId(task.getAuditId());
			updateAdditionalInfo();
			List<AtcoStageVehicle> atcoVehicles = getAllLoadedVins();
			if(atcoVehicles == null || atcoVehicles.size() == 0){
				logger.info("No loaded VINs were found. Quitting...");
				return true;
			}
			logger.info("Processing ("+atcoVehicles.size()+") loaded VINs...");
			HashMap<String, Long> loadedVins = new HashMap<String, Long>();
			HashMap<String, AtcoStagePicture> picURLMap = new HashMap<String, AtcoStagePicture>();
			for(AtcoStageVehicle veh: atcoVehicles){
		    	veh.setCountryId(countryId);
			    if(loadSingleInspection(veh)){
			    	// add the vehicle_id to the list.
			    	loadedVins.put(veh.getVin(), veh.getVehicleId());
			    	loadedVins.put(veh.getVin() + "INSPID", veh.getInspectionId());
			    	loadedVins.put(veh.getVin() + "PICFLAG" , veh.getActive());
			    	HashMap<String, AtcoStagePicture> tempUrls = downloadPhotos(veh, task.getSourcePath());
			    	if(tempUrls!= null){
			    		picURLMap.putAll(tempUrls);
			    	}
			    }
			}
			postLoadingInspectionData(task);
			// call rsync now.
			uploadVehiclePhotosUsingRsync(task, loadedVins, picURLMap);
			
			//Load damages_pictures table now that we have both damage id's and picture id's
			linkDamagesWithPhotos(atcoVehicles, ";");
			logger.info("Ordering photos");
			orderingPhotos(atcoVehicles);
			pushVehicleToAuction(atcoVehicles);
			
		}catch(Exception e){
			sendErrorDocument(getOperationsRecipient(),
					null, null, 
					getProcessTitle() + " Upload Failure - Filename: " + task.getSourceFileName(), Arrays.toString(e.getStackTrace()));
			logger.fatal(e);
			logger.fatal(Arrays.toString(e.getStackTrace()));
			return false;
		}
		
		    
		return true;
	}	
	
	public void pushVehicleToAuction(List<AtcoStageVehicle> vehicles){
		try{
			 for(AtcoStageVehicle veh: vehicles){
				 //For TDD car group config with 1107, 1108. won't create new auction record, only update auction status, times and vehicle status.
							
					if (veh.getCarGroupConfigId() != null && (veh.getCarGroupConfigId().equals(new Long(1107)) || veh.getCarGroupConfigId().equals(new Long(1108))||veh.getCarGroupConfigId().equals(new Long(1358))||veh.getCarGroupConfigId().equals(new Long(1359)))){
						if (veh.getSellerOrganizationId() == null) return;
						if (veh.getSystemId() == 3L) {
							return; 
						}
						
						if(veh.getSystemId() != null && veh.getSystemId().equals(new Long(2)) ){
							Long auctionId = (Long)dataBaseServiceAdaptor.queryObject("ActiveAuctionWithValidPriceId", veh.getVehicleId());
							if(auctionId != null){
								HashMap<String, Object> h = new HashMap<String, Object>();
								h.put("carGroup", new Long(veh.getCarGroupConfigId()));
								dataBaseServiceAdaptor.procedureObject("GetAuctionTimes", h);
								Date startTime = new Date(((Timestamp) h.get("startTime")).getTime());
								Date endTime = new Date(((Timestamp) h.get("endTime")).getTime());
								Date resolveTime = new Date(((Timestamp) h.get("resolveTime")).getTime());
								Auction currentAuction = new Auction();
								currentAuction.setAuctionStatusId(new Long(3));
								currentAuction.setOpenTimeStamp(startTime);
								currentAuction.setEndTime(endTime);
								currentAuction.setResolveTime(resolveTime);
								currentAuction.setAuctionId(auctionId);
								logger.info("Updated "+dataBaseServiceAdaptor.updateObject("AuctionStatusAndTimes", currentAuction)+" vehicle auction status and times.");
								logger.info("Updated "+dataBaseServiceAdaptor.updateObject("VehicleStatusToAuctionStatus", veh.getVehicleId())+veh.getVehicleId()+" vehicle status to auction status.");
							}
							else{
								logger.info("Updated "+dataBaseServiceAdaptor.updateObject("VehicleStatusToAwaitingSellerData", veh.getVehicleId())+" vehicle status to awaiting seller data.");
							}
						}
						
						
					}
				}
									
		}catch(Exception e){
			return;
		}
		return;
	}
	
	//Overwrite this if something needs to be done after the transaction.
	// E.g. sending notification email for successful process. 
	// If an exception happens before committing the transaction
	//  and the transaction gets rolled back, the postTransaction will not be executed.
	protected void postTransaction(AtcoStageVehicle veh) throws Exception{}
	
	protected boolean loadSingleInspection(AtcoStageVehicle veh) throws Exception {
		boolean success = true;
		try{
				dataBaseServiceAdaptor.startTransaction();
				if (validateVehicle(veh)) {
					evaluateActivationRules(veh);
					updateAdditionalVehicleInfo(veh);
					processVehicle(veh);
					overwriteVehicleInfo(veh);
					evaluateAuctionRules(veh);	
					notifyInternalSystem(veh);
					postTransaction(veh);
					dataBaseServiceAdaptor.commitTransaction();
				}else{
					success = false;
				}
			
		}catch(Exception e){
			reportError(veh, e.toString());
			success=false;
			logger.fatal(e.toString());    //PPM:80902
		}finally{
			dataBaseServiceAdaptor.endTransaction();
		}
		if(success){
			AdditionalBussinessProcessor(veh);
			postAfterTrasaction(veh);
		}
		return success;
	}
	
	/***
	 * Prepare all the pictures in the md5 ready structure. Copy the current vin pictures
	 * into a temp folder. Rename the pictures into vehicleid_uploadphotoid.jpg
	 * Insert into upload_photos table.
	 * 
	 * Note: This method would not actually copy the images over. It would only prepare the
	 * images into the appropriate structure.
	 * 
	 * @param VehicleId
	 * @param AtcVehicleId
	 * @param sourceDir
	 * @return
	 * @throws Exception
	 */
	protected void uploadVehiclePhotosUsingRsync(TaskBean task, HashMap<String, Long> loadedVins, HashMap<String, AtcoStagePicture> picUrlMap) throws Exception {
		logger.info("Uploading photos using Rsync ...");
		String temp_picture_directory = createTempDir(task.getSourcePath());
		List<File> photos = getRelatedPhotos(task.getSourcePath(), loadedVins);
		List<Picture> rsyncPhotos = renamePictures(photos, loadedVins, temp_picture_directory, picUrlMap);
		loadPhotos(rsyncPhotos, temp_picture_directory);
	}
	
	protected String createTempDir(String sourcePath) throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat(TEMP_PICTURE_DIR_FORMAT, Locale.US);
		sdf.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		String temp_picture_directory = sourcePath + "/temp_" + sdf.format(Calendar.getInstance().getTime());
		if (!DirectoryUtil.createDirectory(temp_picture_directory)) { 
			logger.error("FAILURE: Error while creating temp picture directory");
			throw new Exception("FAILURE: Error while creating temp picture directory");
		}
		return temp_picture_directory;
	}
	
	protected List<File> getRelatedPhotos(String sourcePath, HashMap<String, Long> vins){
		List<File> pictures = new ArrayList<File>();
		DirectoryReader directoryReader = new DirectoryReader();
		List<File> rawFileList = directoryReader.getFileListing(sourcePath);
		for(File file: rawFileList){
			String vin = getVinFromPictureFileName(file.getName());
			if(vins.containsKey(vin)){
				pictures.add(file);
			}
		}
		return pictures;
	}
	
	//Overwrite this 
	protected boolean isPrimaryPicture(String comments){
		return false;
	}
	
	protected void setComment(Picture pic, String filePath, String comment) {
		pic.setComments(filePath);
	}
	
    protected List<Picture> renamePictures(List<File> pictures, HashMap<String, Long> loadedVins, 
    		String photoDir, HashMap<String, AtcoStagePicture> picUrlMap) throws Exception{
    	logger.info("Renaming ("+pictures.size()+") pictures...");
    	// copy the files into the temp folder and rename the file
    	// to vehicleid_xx.jpg
    	String ext = ".jpg";
    	List<Picture> rsyncPhotos = new LinkedList<Picture>();
    	
		SimpleDateFormat sdf = new SimpleDateFormat(TEMP_PICTURE_DIR_FORMAT, Locale.US);
		sdf.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		
    	for(int count=0; count < pictures.size();count++){
    		File picture = pictures.get(count);
    		String vin = getVinFromPictureFileName(picture.getName());
    		String picFile = loadedVins.get(vin) + "_" + (count+1) + "_" + sdf.format(Calendar.getInstance().getTime());
    		FileInputStream in = new FileInputStream(picture);
    		FileOutputStream out =  new FileOutputStream(photoDir + "/" + picFile + ext);
			// Transfer bytes from the file to the ZIP file
			int len;
			while ((len = in.read(byteBuffer)) > 0) {
				out.write(byteBuffer, 0, len);
			}
			out.close();
			in.close();
			Picture pic = new Picture();
			pic.setVehicleId(loadedVins.get(vin));
			pic.setFilename(picture.getName());
			pic.setActive(loadedVins.get(vin + "PICFLAG"));
			if(picUrlMap != null && picUrlMap.containsKey(picture.getName())){
				AtcoStagePicture asp = picUrlMap.get(picture.getName());
				if(asp != null){
					setComment(pic, asp.getFilePath(), asp.getComments());
					if(isPrimaryPicture(asp.getComments())){
						pic.setPrimary(1L);
					}
				}	
			}else{
				pic.setComments(picture.getName());
			}
			pic.setInspectionId(loadedVins.get(vin + "INSPID"));
			pic.setFilePath(picFile + ext);
			pic.setThumbnailPath(picFile + "_th" + ext);
			pic.setDetailPath(picFile + "_dt" + ext);
			rsyncPhotos.add(pic);	
    	}
    	return rsyncPhotos;
    }
    
    protected String getVinFromPictureFileName(String filename) {
    	
    	return filename.split("_")[0];
    }
    
    protected Long getImageOptionFromComment(Picture picture) throws Exception {
    	return null;
    }
    
    protected List<AtcoStagePicture> getPhotoURLsToDownload(AtcoStageVehicle veh) throws Exception{
    	List<AtcoStagePicture> toDownload = (ArrayList<AtcoStagePicture>)dataBaseServiceAdaptor.queryList("DistinctPhotoURLandCommentsToDownload", veh);
    	return toDownload;
    }
    
    protected String getFileName(String vin, SimpleDateFormat sdf) {
    	return vin + "_" + sdf.format(Calendar.getInstance().getTime()) + ".jpg";
    }
    
    protected HashMap<String, AtcoStagePicture> downloadPhotos(AtcoStageVehicle veh, String sourcePath) throws Exception{
		// check the stage table for any pics to download
		// and download the picture.
    	HashMap<String, AtcoStagePicture> picUrlMap = null;
		List<AtcoStagePicture> toDownload = getPhotoURLsToDownload(veh);
		SimpleDateFormat sdf = new SimpleDateFormat(TEMP_PICTURE_DIR_FORMAT, Locale.US);
		sdf.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		
		int numDownloaded = 0;
		if(toDownload != null){
			logger.info("Need to download "+toDownload.size()+" pictures");
			if(toDownload.size() > 0)
			picUrlMap = new HashMap<String, AtcoStagePicture>();
		}
		for(AtcoStagePicture pic: toDownload){
			String fileName = getFileName(veh.getVin(), sdf);
			if(pic.getFilePath() != null && download(pic.getFilePath(), sourcePath, fileName)){
				// update the photo URL in the comments
				numDownloaded++;
				picUrlMap.put(fileName, pic);
			}
		}
		logger.info("Successfully downloaded "+numDownloaded+" pictures");
		
		return picUrlMap;
	}
	
	protected boolean download(String url, String dir, String fileName) {
		InputStream in = null;
		OutputStream out = null;
		try {
			URL u = new URL(url);
			URLConnection uConnection = u.openConnection();
			uConnection.setReadTimeout(60000);
			in = uConnection.getInputStream();
			out = new FileOutputStream(dir + File.separator + fileName);
			in = new BufferedInputStream(in);
			int bytesRead;
			while ((bytesRead = in.read(byteBuffer)) != -1) {
				out.write(byteBuffer, 0, bytesRead);
			}
			return true;
		} catch (UnknownHostException e1) {
			logger.fatal("UNHostExp: fileName is = " + fileName + ", url = " + url + e1);
		} catch (MalformedURLException e2) {
			logger.fatal("MFURLExp: fileName is = " + fileName + ", url = " + url + e2);
		} catch (SocketTimeoutException e3){
			logger.fatal("SocketTimeOutExp: fileName is = " + fileName + ", url = " + url + e3);
		}catch (IOException e4) {
			logger.fatal("IOExp: fileName is = " + fileName + ", url = " + url + e4);
		}finally {
			if (in != null) {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
				} catch (IOException e5) {
					logger.fatal(e5);
				}
			}
		}
		return false;
	}
    
    protected void loadPhotos(List<Picture> rsyncPhotos, String pictureDir)throws Exception{
    	logger.info("Doing Rsync ...");
    	Set<Long> vehicleSet = new HashSet<Long>();
    	Set<Long> vehiclesWithPrimaryPicturesSet = new HashSet<Long>();
		// spray the directory.
		boolean loaded = sprayPhotoAdaptor.publishDirectory(pictureDir, "*.jpg");
		// clean up the directory.
		DirectoryUtil.cleanupRemoveSourcePath(pictureDir);
		if (!loaded) {
			throw new Exception("FAILURE: Error while publishing the picture directory");
		}
		logger.info("Inserting "+rsyncPhotos.size()+" records into pictures table ...");
		// insert the data into the database and clean up data
		for(Picture pic: rsyncPhotos){
			try{
				//check if primary alread exists
				if(pic.getPrimary() != null && pic.getPrimary().equals(1L) && vehiclesWithPrimaryPicturesSet.contains(pic.getVehicleId())){
					pic.setPrimary(null);
				}
				
				Long optionId = getImageOptionFromComment(pic);
				logger.info("Comment = "+pic.getComments()+" optionId = "+optionId);
				if(optionId != null) {
					pic.setImageOption(optionId);
				}
				
				dataBaseServiceAdaptor.addObject("Pictures", pic);
				if(!vehicleSet.contains(pic.getVehicleId())){
					vehicleSet.add(pic.getVehicleId());
				}
				if(pic.getPrimary() != null && pic.getPrimary().equals(1L) && !vehiclesWithPrimaryPicturesSet.contains(pic.getVehicleId())){
					vehiclesWithPrimaryPicturesSet.add(pic.getVehicleId());
				}
			}
			catch(Exception e){
				reportError(pic.getVehicleId()+"", "Failed to insert picture: "+pic.getFilePath()+" due to "+e);		}
		}
		rsyncPhotos = new ArrayList<Picture>();		
    }
	
	protected boolean validateVehicle(AtcoStageVehicle veh) throws Exception { 
		if (veh.getVehicleId() == null) {
			reportError(veh, "VIN is not in the system. The inspection did not get loaded.");
			veh.setExcpTypeReason("VIN is not in the system");
			unprocessedVehicles.add(veh);
			return false;
		}
		if (veh.getSystemId().equals(new Long(4))) {
			reportError(veh, "VIN is already in Post Sales system. The inspection did not get loaded.");
			veh.setExcpTypeReason("VIN is already in Post Sales system");
			unprocessedVehicles.add(veh);
			return false;
		}
		if (veh.getSystemId().equals(new Long(3)) && veh.getVehicleStatusId().equals(new Long (13))) {
			Long highBidId = (Long)dataBaseServiceAdaptor.queryObject("HighBidId", veh.getVehicleId());
			if(highBidId != null){
				reportError(veh, "VIN is currently in auction with some bids. The inspection did not get loaded");
				veh.setExcpTypeReason("VIN is currently in auction with some bids");
				unprocessedVehicles.add(veh);
				return false; 
			}
		}
		if (veh.getInspectionOrgId() == null){
			reportError(veh, "Cannot find an inspection org id.");
			veh.setExcpTypeReason("Cannot find an inspection org id");
			unprocessedVehicles.add(veh);
			return false;
		}
		// validate vin using chrome
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("vin", veh.getVin());
		dataBaseServiceAdaptor.queryObject("ValidateVin", params);
		if(!"Valid VIN".equalsIgnoreCase(params.get("validated"))){
			reportError(veh, params.get("validated"));
			veh.setExcpTypeReason("Invalid VIN");
			unprocessedVehicles.add(veh);
			return false;
		}
		return true;
	}
	
	protected void processVehicle(AtcoStageVehicle veh) throws Exception {
		String vin = veh.getVin();
		Long vehicleId = veh.getVehicleId();
		Long records = null;
		
		records = (long)dataBaseServiceAdaptor.updateObject("DeactivateOldInspection", vehicleId);
		logger.info("Deactivating (" + (records > 0 ? records : 0) + ") old inspection for VIN: " + vin);
		
		records = (long)dataBaseServiceAdaptor.addObject("Inspections", veh);
		logger.info("Inserting into INSPECTIONS table (inspectionId=" + (records > 0 ? records : 0) + ") for VIN: " + vin);		
		
		try{
			records = (long)dataBaseServiceAdaptor.updateObject("InspectionsTires", veh);
			logger.info("Inserting into InspectionsTires (" + (records > 0 ? records : 0) + ") for VIN: " + vin);
			
			records = (long)dataBaseServiceAdaptor.updateObject("DamageAreas", veh);
			logger.info("Inserting (" + (records > 0 ? records : 0) 
					+ ") New damage areas into DAMAGE_AREAS table for VIN: " + vin);
			
			records = (long)dataBaseServiceAdaptor.updateObject("DamageTypes", veh);
			logger.info("Inserting (" + (records > 0 ? records : 0)  
					+ ") New damage types into DAMAGE_TYPES table for VIN: " + vin);
		}catch(Exception e){
			//production patch #91526, duplicated records for same vin will cause some unique key error in DB, avoid this exception and do not load
			logger.info("duplicated inspection records for vin: " + vin);
		}
		//Update mileage and Exterior color for USB Repo vehicles
		if(new Long(239162L).equals(veh.getSellerOrganizationId()) && new Long(307L).equals(veh.getCarGroupConfigId())){
			if(veh.getInspectionId() == null){
				throw new Exception("Error: Inspection id is null. Cannot update vehicle mileage for US Bank Repo vehicle "+veh.getVehicleId());
			}
			logger.info("Updated ("+dataBaseServiceAdaptor.updateObject("VehicleMileageFromInspection", veh)+") mileage");
		}
		// update mileage for Financialinx repo vehicles 1. only has inspection uploaded, 2. has mvda uploaded before inspection(use dummy_seller_org 247461)
		if((new Long(247461L).equals(veh.getSellerOrganizationId()))){
			if(veh.getInspectionId() == null){
				throw new Exception("Error: Inspection id is null. Cannot update vehicle mileage for Financialinxe  Repo vehicle "+veh.getVehicleId());
			}			
			logger.info("Updated ("+dataBaseServiceAdaptor.updateObject("VehicleMileageFromInspection", veh)+") mileage for Financialinx");
		}
		
		//records = dataBaseServiceAdaptor.updateObject("AddDamages", veh); 
		//logger.info("Inserting into INSPECTION_DAMAGES table (" 
		//		+ (records > 0 ? records : 0) + ") for VIN: " + vin);
		
		//B-14158. Changed to insert damages and related damages_pictures
		List<Damage> damages = (List<Damage>)dataBaseServiceAdaptor.queryList("DamagesForInsert", veh);
		for(Damage damage:damages){
			dataBaseServiceAdaptor.addObject("AddSingleDamage", damage);
		}
		logger.info("Inserting into INSPECTION_DAMAGES table (" + damages.size() + ") for VIN: " + vin);
		veh.setDamages(damages);
	}

	protected void updateAdditionalInfo() throws Exception {
		//  Method to update all inspections
	}
	
	protected void overwriteVehicleInfo(AtcoStageVehicle veh) throws Exception{
		// Overwrite any information for vehicle table, seller specific.
	}
	
	protected void notifyInternalSystem(AtcoStageVehicle veh) throws Exception{
		// overwrite later on the children class
	}

	protected void evaluateActivationRules(AtcoStageVehicle veh) throws Exception{
		// when loading inspection; always activate inspection
		veh.setActive(1L);
	}
	
	protected void setChromeStyleId(AtcoStageVehicle veh) throws Exception {
		HashMap<String, Object> param = new HashMap<String, Object>();
		param.put("vin", veh.getVin());
		// dataBaseServiceAdaptor.queryList("DecodeVin", param);
		chromeResolveUtil.decodeVin(param, null, this.getClass().getName());
		ArrayList<HashMap<String, Object>> decoded  = (ArrayList<HashMap<String, Object>>)param.get("decodedVins");
		if(decoded.size() == 1){
			HashMap<String, Object> temp = decoded.get(0);
			String chromeData = (String)temp.get("CHROME_DATA");
			if("true".equalsIgnoreCase(chromeData)){//Got Chrome style id
				//Create vehicle_additional_info if it doesn't exist
				dataBaseServiceAdaptor.addObject("ChromeIdIntoVehicleAdditionalInfo", veh); 
				BigDecimal chromeId = (BigDecimal)temp.get("STYLE_ID");
				if(chromeId != null){
					veh.setChromeId(chromeId.toString());
					logger.info("Updated "+dataBaseServiceAdaptor.updateObject("ChromeIdForVehicle", veh)+" chrome id for vehicle_id "+veh.getVehicleId());
				}
			}
		}
		
	}
	
	protected void updateAdditionalVehicleInfo(AtcoStageVehicle veh) throws Exception {
		//  Method to update specfic inspections or vehicle
		String vin = veh.getVin();
		Long vehicleId = veh.getVehicleId();		
		Long records = null;
		if(getUpdateMakeModelIfNotSetFlag(veh.getSellerOrganizationId())){
			records = (long)dataBaseServiceAdaptor.updateObject("VehiclesAttributesIfNotSet", veh) ;			
		}
		else if(getMakeModelOverrideFlag(veh.getSellerOrganizationId()))
			records = (long)dataBaseServiceAdaptor.updateObject("VehiclesButDoNotOverrideMMSSAttributes", veh) ;
		else
			records = (long)dataBaseServiceAdaptor.updateObject("VehiclesAttributes", veh) ;
		logger.info("Updating VEHICLES table attributes (" 
				+ (records > 0 ? records : 0) + ") for VIN: " + vin);
		
		HashMap<String, Object> param = new HashMap<String, Object>();
		param.put("vin", vin);
		// dataBaseServiceAdaptor.queryList("DecodeVin", param);
		chromeResolveUtil.decodeVin(param, vehicleId, this.getClass().getName());
		ArrayList<HashMap<String, Object>> decoded  = (ArrayList<HashMap<String, Object>>)param.get("decodedVins");
		if(decoded.size() == 1){
			HashMap<String, Object> temp = decoded.get(0);
			temp.put("VEHICLE_ID", vehicleId);
			records = (long)dataBaseServiceAdaptor.updateObject("VehiclesAttributesUsingVinDecoder", temp);
			logger.info("Updating VEHICLES table attributes ("
					+ (records > 0 ? records : 0) + ") from Chrome for VIN: " + vin);
		}
		if(chromeResolveUtil != null) chromeResolveUtil.decodeVinAndSaveAttributes(veh.getVehicleId());
		
				
		records = (long)dataBaseServiceAdaptor.updateObject("VehiclesParts", veh);
		logger.info("Inserting into VEHICLES_PARTS table (" + (records > 0 ? records : 0) + ") for VIN: " + vin);		
	}	
		
	protected void evaluateAuctionRules (AtcoStageVehicle veh) throws Exception {
		// add a query to check for the config. Default the config to a "Y"
		HashMap<String, Long> param = new HashMap<String, Long>();
		param.put("objectTypeId", 10L);
		param.put("sellerOrgId", veh.getSellerOrganizationId());
		param.put("carGroupConfigId", veh.getCarGroupConfigId());
		Long releaseToAuction = (Long)dataBaseServiceAdaptor.queryObject("ReleaseToAuctionConfig", param);
		if(releaseToAuction != null && releaseToAuction == 0L){
			if(veh.getSystemId().equals(new Long(2)) && veh.getVehicleStatusId().equals(new Long(10))){
			   dataBaseServiceAdaptor.updateObject("SetVehicleToAwaitingSellerRelease", veh.getVehicleId());
			}
			return;
		}
		//2,9 -> 2.14
		if(veh.getSystemId().equals(new Long(2)) &&
				(veh.getVehicleStatusId().equals(new Long(9)) || veh.getVehicleStatusId().equals(new Long(22)))){
			logger.info("Updated ("+dataBaseServiceAdaptor.updateObject("SetVehicleToAwaitingPricing", veh.getVehicleId())+") vehicle status from 2,9 to 2,14");			
		}
		else if(veh.getSystemId().equals(new Long(2)) &&
				(veh.getVehicleStatusId().equals(new Long(12)) || 
				 veh.getVehicleStatusId().equals(new Long(10)))){
			logger.info("Updated ("+dataBaseServiceAdaptor.updateObject("MoveVehicleToAuction", veh.getVehicleId())+") vehicle status from 2,10 or 2,12 to 3,13");
			updateAuction(veh);
		}
	}
	
	protected boolean setInspectionOrgId(TaskBean task){
		try {
			List<AtcoStageVehicle> info = (List)dataBaseServiceAdaptor.queryList("InspectionOrgBySellerOrg", getSellerOrgId());
			if(info != null && info.size() > 1){
				// need to determine the details of the info object later. 
				setInfo(null);
				return true;
			}else if(info != null && info.size() == 1){
				AtcoStageVehicle obj = info.get(0);
				setInspectionOrgId((Long) obj.getInspectionOrgId());
				setInfo(obj);
				return true;
			}
			else{
				String subject = getProcessTitle() + " Upload Failed - Filename: " + task.getSourceFileName();
				String body = "Please check DS2K7_OBJECT_REFERENCE_MAPS table for SELLER_ORGANIZATION_ID = " 
					+ getSellerOrgId() + " and OBJECT_TYPE_ID = 3\n\n";
				sendErrorDocument(getOperationsRecipient(),null, null, subject, body);
				logger.error("Cannot find the inspection_seller_org_id in the DS2K7_OBJECT_REFERENCE_MAPS TABLE, no inspections loaded. Email was sent");
				return false;
			}
		} catch (Exception e) {
			sendErrorDocument(getOperationsRecipient(),
					null, null, 
					getProcessTitle() + " Upload Failure - Filename: " + task.getSourceFileName(), e.toString());
			e.printStackTrace();
			logger.error(e);
			return false;
		}		
	}
	
	protected void updateAuction(AtcoStageVehicle veh) throws Exception {
		dataBaseServiceAdaptor.updateObject("SetVehicleToAuction", veh.getVehicleId().longValue());
		Auction auction = new Auction();
		
		Long carGroupConfigID = veh.getCarGroupConfigId();
		if(carGroupAuctionTimes.get(carGroupConfigID) == null){
			carGroupAuctionTimes.put(carGroupConfigID, AuctionUtil.getAuctionTime(carGroupConfigID, dataBaseServiceAdaptor));
		}
		AuctionRelease ar = carGroupAuctionTimes.get(carGroupConfigID);
		auction.setOpenTimeStamp(ar.getStartTime());
		auction.setEndTime(ar.getEndTime());
		auction.setResolveTime(ar.getResolveTime());
		auction.setVehicleId(veh.getVehicleId());
		dataBaseServiceAdaptor.updateObject("AuctionTimes", auction);			
	}	
	
	protected boolean linkDamagesWithPhotos(List<AtcoStageVehicle> atcoVehicles, String picutreUrlSeperator)throws Exception{
		for(AtcoStageVehicle veh: atcoVehicles){
			logger.info("Linking pictures to damages for VIN: "+veh.getVin());
			if(veh.getDamages() == null || veh.getDamages().size() == 0) continue;//In case damages were never set
			for(Damage damage:veh.getDamages()){
				String pictureURLs = damage.getDamagePictureURLs();
				if(pictureURLs == null) continue;
				String[] pictureURLArray = pictureURLs.split(picutreUrlSeperator);
				int count = 0;
				for(String pictureURL:pictureURLArray){
					if(pictureURL.length() == 0) continue; //Skip empty elements
					if(damage.getDamageId() == null) continue; //Skip when no damage_id		
					damage.setDamagePictureURLs(pictureURL);
					dataBaseServiceAdaptor.addObject("AddDamagePicture", damage);
					count++;
				}
				logger.info("  >> Linked "+count+" pictures to damage id: "+damage.getDamageId());
			}
		}
		return true;
		
	}

	protected boolean orderingPhotos(List<AtcoStageVehicle> atcoVehicles)
			throws Exception {		
		List<Picture> pics;
		List<Picture> damagePics;		
		final String FC_REGEX = "^.*FC_\\d+\\.JPG|^.*FC.JPG";
		final String FRNT_REGEX = "^.*FRNT_\\d+\\.JPG|^.*FRNT.JPG";
		Pattern pattern;
		Matcher matcher;
		Pattern pattern_frnt;
		Matcher matcher_frnt;
		
		for (AtcoStageVehicle veh : atcoVehicles) {
			int ordinalPosition = 2;
			if (veh.getInspectionId() == null)
				continue;
			pics = (List) dataBaseServiceAdaptor.queryList("NonDamagePictures", veh);
			for (Picture photo : pics) {
		        
				pattern = Pattern.compile(FC_REGEX,Pattern.CASE_INSENSITIVE);
		        matcher = pattern.matcher(photo.getComments());
				pattern_frnt = Pattern.compile(FRNT_REGEX,Pattern.CASE_INSENSITIVE);
		        matcher_frnt = pattern_frnt.matcher(photo.getComments());		        

				if (matcher.matches()) {
					photo.setOrdinalPosition(0L);								
				} else if (matcher_frnt.matches()) {
					photo.setOrdinalPosition(1L);
				} else {
					photo.setOrdinalPosition(new Long(ordinalPosition++));
				}
				dataBaseServiceAdaptor.updateObject("OrdinalPosition",photo);
			}
			
			damagePics = (List) dataBaseServiceAdaptor.queryList("DamagePictures", veh);
			
			for (Picture damagePhoto : damagePics) {
				damagePhoto.setOrdinalPosition(new Long(ordinalPosition++));
				damagePhoto.setPrimary(null);
				dataBaseServiceAdaptor.updateObject("OrdinalPosition",damagePhoto);
			}
			//Setting primary
			try{
				   dataBaseServiceAdaptor.updateObject("SetPrimaryPictureByVehicleId", veh.getVehicleId());
				   dataBaseServiceAdaptor.updateObject("FixPrimaryPictureByVehicleId", veh.getVehicleId());
				}
				catch(Exception e){
					reportError(veh.getVehicleId()+"", "Failed to set primary picture.");
				}
		}
		return true;
	}
	
	public void deleteDuplicateVin(Long auditId) throws Exception {
		logger.info("Deleting the "+dataBaseServiceAdaptor.updateObject("RejectAtcoDuplicateVins", auditId)+" duplicate VIN's");
	}
	protected boolean validateFileContent(TaskBean task) throws Exception {
		
		String error = "";
		Long auditId = task.getAuditId();
		
		List invalidVinSet = (List)dataBaseServiceAdaptor.queryList("InvalidVins", auditId);
		Iterator invalidVins = invalidVinSet.iterator();
		while(invalidVins.hasNext()){
			String vin = (String)invalidVins.next();
			//error += (vin == null? ", Vin cannot be null.\n" : vin + ", Duplicate Vin\n");
			error += vin + ", Invalid VIN\n";//Case 17887: reject duplicate VINs, load others
		}
		List inspectionSet = (List)dataBaseServiceAdaptor.queryList("AtcoStageInspections", auditId);
		Iterator inspectionIterator = inspectionSet.iterator();
		while(inspectionIterator.hasNext()){
			AtcoStageInspection inspection = (AtcoStageInspection)inspectionIterator.next();
			applyPreReq(inspection);
			if(inspection.getInspectionDate() == null){
				error += inspection.getVin() + ", Inspection date cannot be null.\n";
			}else if(inspection.getInspectionMileage() == null){
				error += inspection.getVin() + ", Inspection mileage cannot be null.\n";
			}else if (inspection.getOdometerCondition() == null || 
					inspection.getOdometerCondition().length() == 0 ){
				error += inspection.getVin() + ", Odometer Condition has to be 'Good', 'TMU' or 'Replaced.'\n";
			}else if (inspection.getFrameDamage() == null){
				error += inspection.getVin() + ", Frame Damage has to be 'Y' or 'N'\n";
			}
		}
		List damageSet = (List)dataBaseServiceAdaptor.queryList("AtcoStageDamages", auditId);
		Iterator damageSetIterator = damageSet.iterator();
		while(damageSetIterator.hasNext()){
			AtcoStageDamage damage = (AtcoStageDamage) damageSetIterator.next();
			if((damage.getDamageArea() == null && damage.getDamageType() == null && damage.getRepairCost() == null)){
				error += damage.getVin() + ", All 3 fields (Damage Area, Damage Type, RepairCost) need to be populated\n";
			}
		}
		deleteDuplicateVin(auditId);
		if(error.length() > 0){
			error = "VIN, Error reason\n" + error;
			String subject = getProcessTitle() + " Upload Failed - Filename: " + task.getSourceFileName();
			String body = "Please fix all the errors in the attachment file and run the inspection again.";
			String errorFile = task.getSourcePath() + File.separator + "error.csv";
			fileAdaptor.saveToFile(errorFile, error);
			sendErrorDocument(getSellerRecipient(),
					"eror.csv", errorFile, subject, body);
			logger.error("The inspection have error, no inspections loaded. Email was sent");
			logger.error(error);
			return false;
		}
		return true;
	}
	
	protected void applyPreReq(AtcoStageInspection inspection) throws Exception{
		// Overwrite by inherited classes for any additional pre requisites before
		// the validation happens.
		
	}

	protected boolean deleteAllStagingTables() throws Exception{
		Long sellerOrgId = getSellerOrgId();
		try {
			dataBaseServiceAdaptor.startTransaction();
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageVehicles",sellerOrgId) 
					+ " AtcoStageVehicles records deleted for sellerOrgId = " 
					+ sellerOrgId );
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageInspections",sellerOrgId) 
					+ " AtcoStageInspections records deleted for sellerOrgId = " 
					+ sellerOrgId );
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageTires",sellerOrgId) 
					+ " AtcoStageTires records deleted for sellerOrgId = " 
					+ sellerOrgId );
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageDamages",sellerOrgId) 
					+ " AtcoStageDamages records deleted for sellerOrgId = " 
					+ sellerOrgId );
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageParts",sellerOrgId) 
					+ " AtcoStageParts records deleted for sellerOrgId = " 
					+ sellerOrgId );	
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStagePictures",sellerOrgId) 
					+ " AtcoStagePictures records deleted for sellerOrgId = " 
					+ sellerOrgId );	
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageLocations",sellerOrgId) 
					+ " AtcoStageLocations records deleted for sellerOrgId = " 
					+ sellerOrgId );
			logger.info(dataBaseServiceAdaptor.deleteObject("AtcoStageInspectionAddInfos",sellerOrgId) 
					+ " AtcoStageInspectionAddInfos records deleted for sellerOrgId = " 
					+ sellerOrgId );
			dataBaseServiceAdaptor.commitTransaction();
		} catch (Exception e) {
			logger.error("Something crash during deleting all the staging tables");
			logger.fatal(e);
			sendErrorDocument(getOperationsRecipient(),null, null, 
					"DS2K7 - " + getProcessTitle() + " Upload Failed!", 
			"Something crash during deleting all the staging tables, please check the log file for details." );
			return false;
			
		} finally {
			dataBaseServiceAdaptor.endTransaction();			
		}
		return true;
		
	}
	protected boolean cleanAllStagingTables(AtcoStageVehicle asv) throws Exception{
		try {
			dataBaseServiceAdaptor.startTransaction();
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageVehicles",asv) 
					+ " AtcoStageVehicles records deleted for sellerOrgId = " 
					+ asv.getSellerOrganizationId() +" and inspectionOrgId="+ asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageInspections",asv) 
					+ " AtcoStageInspections records deleted for sellerOrgId = " 
					+  asv.getSellerOrganizationId()+" and inspectionOrgId="+ asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageTires",asv) 
					+ " AtcoStageTires records deleted for sellerOrgId = " 
					+  asv.getSellerOrganizationId()+" and inspectionOrgId="+ asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageDamages",asv) 
					+ " AtcoStageDamages records deleted for sellerOrgId = " 
					+  asv.getSellerOrganizationId()+" and inspectionOrgId="+ asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageParts",asv) 
					+ " AtcoStageParts records deleted for sellerOrgId = " 
					+  asv.getSellerOrganizationId()+" and inspectionOrgId="+ asv.getInspectionOrgId());	
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStagePictures",asv) 
					+ " AtcoStagePictures records deleted for sellerOrgId = " 
					+  asv.getSellerOrganizationId()+" and inspectionOrgId="+ asv.getInspectionOrgId());	
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageLocations",asv) 
					+ " AtcoStageLocations records deleted for sellerOrgId = " 
					+  asv.getSellerOrganizationId() +" and inspectionOrgId="+ asv.getInspectionOrgId());	
			dataBaseServiceAdaptor.commitTransaction();
		} catch (Exception e) {
			logger.error("Something crash during deleting all the staging tables");
			logger.fatal(e);
			sendErrorDocument(getOperationsRecipient(),null, null, 
					"DS2K7 - " + getProcessTitle() + " Upload Failed!", 
			"Something crash during deleting all the staging tables, please check the log file for details." );
			return false;
			
		} finally {
			dataBaseServiceAdaptor.endTransaction();			
		}
		return true;
		
	}


	protected boolean setSellerOrgIdByFileName(String fileName){
		try{
			Long sellerOrgId = (Long)dataBaseServiceAdaptor.queryObject("SellerOrgIdByFileName", fileName);
			if(sellerOrgId != null){
				setSellerOrgId(sellerOrgId);
				return true;
			}
		}
		catch(Exception e){
			logger.fatal(e);
			sendErrorDocument(getOperationsRecipient(),
					null, null, 
					getProcessTitle() + " Upload Failure - Filename: " + fileName, e.toString());
			return false;			
		}
		return false;
	}
	
	protected boolean setSellerOrgId(String taskFile){
		NodeList nodelist = null;
		
		try {
			String xpath = "//file/VehicleSet/*";
			Document document = xmlAdaptor.getDocumentFromFile(taskFile);
			nodelist = XPathAPI.selectNodeList(document, xpath);	
		} catch (javax.xml.transform.TransformerException e) {
			logger.fatal(e);
			sendErrorDocument(getOperationsRecipient(),
					null, null, 
					getProcessTitle() + " Upload Failure - Filename: " + taskFile, e.toString());
			return false;
		}
		
		int i;
		for (i = 0; i<nodelist.getLength(); i++) {
			
			Node node = (Node)nodelist.item(i);				
			HashMap<String, String> dataMap = XMLJavaHelper.xmlNodeToMap(node);
			String vin = dataMap.get("Vin");
			
			try {
				List sellerOrgIds = (List)dataBaseServiceAdaptor.queryList("SellerOrgByVin", vin);
				if(sellerOrgIds.size() == 1){
					setSellerOrgId((Long)sellerOrgIds.get(0));
					logger.info("SellerOrgId (" + getSellerOrgId() + ") established from VIN: " + vin);					
					return true;
				}
			} catch (Exception e) {
				logger.fatal("Exception", e);
				sendErrorDocument(getOperationsRecipient(),
						null, null, 
						getProcessTitle() + " Upload Failure - Filename: " + taskFile, e.toString());
				return false;
			}			
		}
		return false;
	}
	
	protected boolean loadAllTables(TaskBean task, DatabaseTargetBean destination)throws Exception{
		// Note: might be a good idea to move this for loop into a seperate function.
		String [] ctlFiles = destination.getBatchControlFileMapping().split(",");
		String dataFileName = null;
		String xPath = null;
		String delimiter = null;
		String append = null;
		for(String ctlFileName:ctlFiles){
			if(ctlFileName.indexOf("vehicles") > 0){
				logger.info("Bulk LOADING into atco_stage_vehicles table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".vehicles";
				xPath = "//file/VehicleSet/*";
				delimiter = "|";
			}
			else if(ctlFileName.indexOf("inspections") > 0){
				logger.info("Bulk LOADING into atco_stage_inspections table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".inspections";
				xPath = "//file/InspectionSet/*";
				delimiter = "|";
			}
			else if(ctlFileName.indexOf("tires") > 0){
				logger.info("Bulk LOADING into atco_stage_tires table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".tires";
				xPath = "//file/TireSet/*";
				delimiter = "|";
			}
			else if(ctlFileName.indexOf("damages") > 0){
				logger.info("Bulk LOADING into atco_stage_damages table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".damages";
				xPath = "//file/DamageSet/*";
				delimiter = "|";
			}
			else if(ctlFileName.indexOf("parts") > 0){
				logger.info("Bulk LOADING into atco_stage_parts table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".parts";
				xPath = "//file/PartSet/*";
				delimiter = "|";
			}
			else if(ctlFileName.indexOf("pictures") > 0){
				logger.info("Bulk LOADING into atco_stage_pictures table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".pictures";
				xPath = "//file/PictureSet/*";
				delimiter = "|";
			}
			else if(ctlFileName.indexOf("locations") > 0){
				logger.info("Bulk LOADING into atco_stage_locations table: " + task.getTaskFile());
				dataFileName = task.getSourceFile()+".locations";
				xPath = "//file/LocationSet/*";
				delimiter = "|";
			}
			
			if(isAppendStaticOrgIds()){
			   append = delimiter + getSellerOrgId().toString() 
			   + delimiter + (getInspectionOrgId() == null? "" : getInspectionOrgId().toString());
			}
			if(append != null){
				append +=  delimiter + task.getAuditId();
			}else{
				append = delimiter + task.getAuditId();
			}
			// Do not continue if we fail to load either vehicles or inspections.
			// If any other tables fail to load, it is okay to continue.
			if(!loadData(task.getTaskFile(), xPath, append, dataFileName, ctlFileName.trim(), delimiter)){
				String subject = getProcessTitle() + " Upload Failed - Filename: " + task.getSourceFileName();
				String body = "SQLLDR failed during loading one of the majors table - ATCO_STAGE_VEHICLES or ATCO_STAGE_INSPECTIONS.\n" +
				"SQLLDR log file was sent in a separate email. Please check the error and run the report again";
				sendErrorDocument(getOperationsRecipient(),
						task.getSourceFileName(), task.getSourceFile(), subject, body);
				logger.info("SQLLDR failed during loading, no inspections loaded. Email was sent");
				return false;
			}
			
		}
		// if we have multiple mapping for vehicles, then we should overwrite each inspection org id
		
		//if(info == null){ //info is changed to getInfo(). The attribute 'info' is redelcared
		                    //in a few subclasses, e.g. Manders, and BMWFS. As loadAllTables()
		                    // belongs to the parent class, thus it's working on the parent version 
		                    // of the 'info', which never never gets set
		                    // in the subclasses, therefore 'info' here is always null. 
		                    // It's confusing and problematic in this case
		                    // that the subclasses redeclare the attribute. But for now it's easier
		                    // to fix it here rather than fixing all subclasses, which have been tested.
		
		return true;
	}
	
	protected boolean overwriteInfoObj(Long auditId) throws Exception{
		if(getInfo() == null){
			// means we have multiple mapping. so look up via IRT table to figure out the inspection org id
			logger.info(dataBaseServiceAdaptor.updateObject("InspectionOrgAtcoStageVehicles",auditId) 
					+ " AtcoStageVehicles.Inspection_org_id records updated for auditId = " 
					+ auditId);
			logger.info(dataBaseServiceAdaptor.updateObject("InspectionOrgAtcoStageInspections",auditId) 
					+ " AtcoStageInspections.Inspection_org_id records updated for auditId = " 
					+ auditId);
			logger.info(dataBaseServiceAdaptor.updateObject("InspectionOrgAtcoStageTires",auditId) 
					+ " AtcoStageTires.Inspection_org_id records updated for auditId = " 
					+ auditId);
			logger.info(dataBaseServiceAdaptor.updateObject("InspectionOrgAtcoStageDamages",auditId) 
					+ " AtcoStageDamages.Inspection_org_id records updated for auditId = " 
					+ auditId);
			logger.info(dataBaseServiceAdaptor.updateObject("InspectionOrgAtcoStageParts",auditId) 
					+ " AtcoStageParts.Inspection_org_id records updated for auditId = " 
					+ auditId);
			AtcoStageVehicle veh = new AtcoStageVehicle();
			//veh.setSellerOrganizationId(sellerOrgId); //sellerOrgId is redeclared in many subclasses. See comments above.
			veh.setSellerOrganizationId(getSellerOrgId());
			// do not set the car group config id or inspection org id
			setInfo(veh);
		}
		return true;
	}
	
	protected boolean loadData(String taskFile, String xPath, String append, String sqlLdrFileName, String ctlFileName, String delimiter){
		Document document = xmlAdaptor.getDocumentFromFile(taskFile);
		String formattedData = fileAdaptor.getSQLLDRFileData(document, xPath, append, delimiter);
		
		// based on intent, sqlLDRAdaptor identifies loading no data as a fatal error
		// so do not load optional info if not available
		if(formattedData.length() < 1) {
			if (sqlLdrFileName.endsWith(".tires") || sqlLdrFileName.endsWith(".damages") ||
				sqlLdrFileName.endsWith(".parts") || sqlLdrFileName.endsWith(".pictures") || 
				sqlLdrFileName.endsWith(".inspectionAdditional")){
				return true;
			}
		}		
		
		String dataFile = sqlLdrFileName+".phase3.data";
		if (fileAdaptor.saveToFile(dataFile, formattedData)) {
			String badFile = dataFile+".sqlldr.bad";
			String logFile = dataFile+".sqlldr.log";
			boolean loaded = sqlLDRAdaptor.loadFile2(registryBean.getConfigDir(),
					ctlFileName,dataFile, badFile, logFile, true);
			if (!loaded) {
				logger.info("Bulk LOADING Failed: " + taskFile);
				sendErrorDocument(getOperationsRecipient(),
						sqlLdrFileName+".phase3.data.sqlldr.log", logFile,
						"ATCO DS2K7 was unable to load or update attached record for - " + getProcessTitle(), null);
				return false;
			}
			else{//Send the bad file if it exists.
				if ((new File(badFile)).exists() && new File(badFile).length() > 0L) {
					logger.info("SQLLDR bad file found. Sending it to errorRecipient...");
					sendErrorDocument(getOperationsRecipient(),
							sqlLdrFileName+".phase3.data.sqlldr.bad", badFile,
							"ATCO DS2K7 was unable to load or update attached record for - " + getProcessTitle(), null);
				}			
			}
		}
		return true;
	}	
	
	protected boolean reportUnprocessedVehicles(TaskBean task) throws Exception{
		reportDuplicateVins(task);
		if(reportBuffer.length() > 0){
			reportBuffer.insert(0,"VIN, Error Reason\n");
			String subject = "Unprocessed Vehicle(s) in " + getProcessTitle() + " Upload - Filename: " + task.getSourceFileName();
			String body = "The attached records were not loaded due to errors";
			String errorFile = task.getSourcePath() + File.separator + "error_report.csv";
			fileAdaptor.saveToFile(errorFile, reportBuffer.toString());
			sendErrorDocument(getSellerRecipient(),
					"error_report.csv", errorFile, subject, body);
			logger.info("Errors occurred during loading. All the un-loaded records are sent in the email.");
			logger.info(reportBuffer.toString());
		}
		
		if(additionalErrorRecipient != null && !unprocessedVehicles.isEmpty()){
			logger.info("Sending error report to "+additionalErrorRecipient);
			StringBuffer errorReport = new StringBuffer();
			errorReport.append("VIN,Inspection Company,Vehicle Location,Error Reason\n");
			for(AtcoStageVehicle veh:unprocessedVehicles){
				String vehicleLocation = null;
				if(veh.getLocationId() != null)
					vehicleLocation = (String)dataBaseServiceAdaptor.queryObject("LocationStringByLocationId", veh.getLocationId());
				errorReport.append(veh.getVin()+","+inspectionOrgName+","+vehicleLocation+","+veh.getExcpTypeReason()+"\n");
			}
			String subject = "Inspection Upload Error Report ("+inspectionOrgName+")";
			String body = "Errors occured when loading inspections for Chrysler vehicles. The attached VIN's are not loaded.";
			String errorFile = task.getSourcePath() + File.separator + "error_report.csv";
			fileAdaptor.saveToFile(errorFile, errorReport.toString());
			sendErrorDocument(additionalErrorRecipient,
					"error_report.csv", errorFile, subject, body);
			logger.info(errorReport.toString());
			
		}
		
		return true;
	}

	protected void reportDuplicateVins(TaskBean task) throws Exception{
		List<String> duplicateVins = (List<String>)dataBaseServiceAdaptor.queryList("DuplicateVins", task.getAuditId());
		for(String vin:duplicateVins){
			reportError(vin, "Duplicate VIN");			
		}
	}
	
	protected void reportError(AtcoStageVehicle veh, String message){
		reportBuffer.append(veh.getVin() + ", "+message+"\n"); 
		logger.error(veh.getVin() + ", "+message);
		uploadReportBuffer.append(veh.getVin()+","+DateTimeUtil.getTimeStamp(Calendar.getInstance(), REPORT_DATE_FORMAT)+","+veh.getVehicleStatusName()+",Failure: "+message+"\n");		
	}
	
	protected void reportError(String vin, String message){
		reportBuffer.append(vin + ", "+message+"\n"); 
		logger.error(vin + ", "+message);
		uploadReportBuffer.append(vin+","+DateTimeUtil.getTimeStamp(Calendar.getInstance(),REPORT_DATE_FORMAT)+",,Failure: "+message+"\n");		
	}
	
	protected void sendUploadReport(TaskBean task, String subject, String reportFileName) throws Exception{
		String reportData = "VIN,TIME_STAMP,STATUS,RESULT\n"+uploadReportBuffer;
		String body = "Status of the vehicles sent in the feed (After loading)";
		String reportFile = task.getSourcePath() + File.separator + reportFileName;
		fileAdaptor.saveToFile(reportFile, reportData);
		sendErrorDocument(getUploadReportRecipient(),
				reportFileName, reportFile, subject, body);
		logger.info(body);
		logger.info(reportData);	
	}
	
	//Utility method to update vehicle status
	protected void setVehicleStatus(AtcoStageVehicle veh, VehicleStatusId vehicleStatusId, String message) throws Exception {
		veh.setSystemId(vehicleStatusId.getSystemIdValue());
		veh.setVehicleStatusId(vehicleStatusId.getValue());
		veh.setVehicleStatusName(vehicleStatusId.name());
		dataBaseServiceAdaptor.updateObject("VehicleStatus", veh);
		logger.info("Updated status for VIN: "+veh.getVin()+" to ("+veh.getSystemId()+", "+veh.getVehicleStatusId()+"): "+message);									
	}
	
	protected boolean sendEmail(String toRecipient, String subject, String body) {
		Properties appProps = propertiesAdaptor.getProps();
		if(body == null)
			body = "Attached is the error records report";
		String to = appProps.getProperty(toRecipient);
		return sendToEmailAddress(to, subject, body);
	}
	
	protected boolean sendToEmailAddress(String to, String subject, String body) {
		Properties appProps = propertiesAdaptor.getProps();
		if(body == null)
			body = "Attached is the error records report";		
		if (to == null) {
			return false;
		}
		else {		
			String from = appProps.getProperty("mailFrom");
			try {
				emailAdaptor.sendErrorNotification(to,
						from,
						subject,
						body,
						null,
						null);
			}
			catch (Exception e) {    
				logger.error("Exception", e);
				return false;
			}  
		}
		return true;
	}
	
	protected boolean sendErrorDocument(String toRecipient, String errorFileName, String errorFile, String subject, String body) {
		Properties appProps = propertiesAdaptor.getProps();
		if(body == null)
			body = "Attached is the error records report";		
		String to = appProps.getProperty(toRecipient);
		if (to == null) {
			return false;
		}
		else {
			String env = appProps.getProperty("dbEnv");			
			if (!"prod1".equalsIgnoreCase(env)){
				try{
					String prodRecipient = (String)dataBaseServiceAdaptor.queryObject("ConfigPropertyValueInProd", toRecipient);
					body = "******** In prod this email would be sent to "+prodRecipient+" *************\n\n"+body;
				}
				catch(Exception e){
					logger.error("Failed to get prod recipeint due to: "+e);
					//Ignore it since it's for testing purpose
				}
			}
			String from = appProps.getProperty("mailFrom");
			try {
				emailAdaptor.sendErrorNotification(to,
						from,
						subject,
						body,
						errorFileName,
						errorFile);
			}
			catch (Exception e) {    
				logger.error("Exception", e);
				return false;
			}  
		}
		return true;
	}
	
	protected boolean getMakeModelOverrideFlag(Long sellerId) throws Exception{
		HashMap<String,Object> param = new HashMap<String,Object>();
		param.put("sellerOrganizationId", sellerId);
		param.put("propertyName", "makeModelSeriesStyleOverride");
		String config = (String)dataBaseServiceAdaptor.queryObject("PropertyBySellerOrg", param);
		if(config != null && "Y".equals(config))
			return true;
		else
			return false;
	}
	
	protected boolean getUpdateMakeModelIfNotSetFlag(Long sellerId) throws Exception{
		HashMap<String,Object> param = new HashMap<String,Object>();
		param.put("sellerOrganizationId", sellerId);
		param.put("propertyName", "updateMakeModelSeriesStyleIfNotSet");
		String config = (String)dataBaseServiceAdaptor.queryObject("PropertyBySellerOrg", param);
		if(config != null && "Y".equals(config))
			return true;
		else
			return false;
	}
	
	protected void runV1Script(String scriptName, List<Long> vehicleIds) throws Exception{
		String vehicleIdStr = "";
		try {
			Properties appProps = propertiesAdaptor.getProps();
			String path = appProps.getProperty("v1ScriptFolder");
			scriptName = path+"/"+scriptName;
			for(Long vehicleId:vehicleIds){
				vehicleIdStr += " "+vehicleId;
			}
			scriptName += vehicleIdStr;
			logger.info("Starting to run V1 Script "+scriptName);
			Process p = Runtime.getRuntime().exec(scriptName);
			logger.info("Done running V1 Script"+p);
			
		}
		catch (Exception e) {
			logger.error("Error in runV1Script: Exception when running "+scriptName, e);
		}
	}
	
	protected void doUpdateImageOption(AtcoStageVehicle veh) throws Exception{
	}

	/**
	 * Default Methods to be Overridden
	 */	
	protected String getProcessTitle() {
		return processTitle;
	}
	protected String getOperationsRecipient() {
		return operationsRecipient;
	}
	protected String getSellerRecipient() {
		return sellerRecipient;
	}
	protected Long getInspectionOrgId() {
		return inspectionOrgId;
	}
	protected void setInspectionOrgId(Long inspectionOrgId) {
		this.inspectionOrgId = inspectionOrgId;
	}
	protected Long getSellerOrgId() {
		return sellerOrgId;
	}
	protected void setSellerOrgId(Long sellerOrgId) {
		this.sellerOrgId = sellerOrgId;
	}
	protected AtcoStageVehicle getInfo() {
		return info;
	}
	protected void setInfo(AtcoStageVehicle info) {
		this.info = info;
	}
	public SprayPhotoAdaptor getSprayPhotoAdaptor() {
		return sprayPhotoAdaptor;
	}
	public void setSprayPhotoAdaptor(SprayPhotoAdaptor sprayPhotoAdaptor) {
		this.sprayPhotoAdaptor = sprayPhotoAdaptor;
	}

	public boolean isAppendStaticOrgIds() {
		return appendStaticOrgIds;
	}

	public void setAppendStaticOrgIds(boolean appendStaticOrgIds) {
		this.appendStaticOrgIds = appendStaticOrgIds;
	}

	public ChromeResolveUtil getChromeResolveUtil() {
		return chromeResolveUtil;
	}

	public void setChromeResolveUtil(ChromeResolveUtil chromeResolveUtil) {
		this.chromeResolveUtil = chromeResolveUtil;
	}

	public String getUploadReportRecipient() {
		return uploadReportRecipient;
	}

	public void setUploadReportRecipient(String uploadReportRecipient) {
		this.uploadReportRecipient = uploadReportRecipient;
	}

	public Long getCountryId() {
		return countryId;
	}

	public void setCountryId(Long countryId) {
		this.countryId = countryId;
	}
	
	protected void postAfterTrasaction(AtcoStageVehicle veh) throws Exception {
		//do nothing just for GMF CDK in subclass
	}
public boolean decodeVinAndSaveAttributesforGmf(Long vehicleId, String vin, Long countryId, boolean overwriteAttributes) {
		
		if(vehicleId == null){
			logger.error("Cannot decode VIN using decodeVinAndSaveAttributes(). Vehicle Id is null.");
			return false;
		}
		try{
			if(vin == null || countryId == null){
				Map<String, Object> resultMap = (HashMap<String, Object>)dataBaseServiceAdaptor.queryObject("VinAndCountryIdForVehicleId", vehicleId);
				vin = (String)resultMap.get("VIN");
				countryId = ((BigDecimal)resultMap.get("COUNTRYID")).longValue();
			}
			
			Map<String,Object> decodedMap=new HashMap<String, Object>();
			if (chromeResolveUtil != null)
			   decodedMap =chromeResolveUtil.decodeVehicleIdToUnique(vin, countryId);
			if(decodedMap != null){
				decodedMap.put("VEHICLE_ID", vehicleId);
				
				
				if(overwriteAttributes)
					logger.info("Updated "+dataBaseServiceAdaptor.updateObject("OverwriteVehicleAttributesUsingVinDecoder", decodedMap)+" attributes from Chrome for Vin: "+vin);
				else 
					logger.info("Updated "+dataBaseServiceAdaptor.updateObject("VehicleAttributesUsingVinDecoderforGmf", decodedMap)+" attributes from Chrome for Vin: "+vin);
				   logger.info("Updated "+dataBaseServiceAdaptor.updateObject("VehicleAdditionalInfoChromeId", decodedMap)+" Vehicle_additional_infos.chrome_id");

			}			
		}
		catch(Exception e){
			logger.error("Failed to decode VIN for vehicle id: "+vehicleId+". Due to: "+e);
			return false;
		}
		return true;
		
	}
	
}

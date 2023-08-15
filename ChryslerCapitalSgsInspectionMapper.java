package com.atc.dataservices.mapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.atc.dataservices.common.SystemId;
import com.atc.dataservices.common.VehicleStatusId;
import com.atc.dataservices.mapper.model.AtcoStagePicture;
import com.atc.dataservices.mapper.model.AtcoStageVehicle;
import com.atc.dataservices.mapper.model.KeyValuePair;
import com.atc.dataservices.model.TaskBean;
import com.atc.dataservices.mapper.lifecycle.ChryslerVehicleLifeCycleManager;

public class ChryslerCapitalSgsInspectionMapper extends AtcoStdSellerInspectionMapper {
	private static Logger logger = Logger.getLogger(ChryslerCapitalSgsInspectionMapper.class);
	private static String processTitle = "Chrysler Capital SGS Inspection";
	private static String sellerRecipient = "chryslerCapitalSgsInspectionUploadReportRecipient";
	protected static final String DATE_FORMAT = "MM/dd/yyyy";
	protected ChryslerVehicleLifeCycleManager chryslerVehicleLifeCycleManager;

	{
		sellerOrgId = 394466L;
//		santanderSCLeaseSellerOrgId = 597577L;
		inspectionOrgId=3L;
		appendStaticOrgIds = false;
		uploadReportRecipient="chryslerCapitalSgsInspectionUploadReportRecipient";
	}
	/**
	 * Override Methods
	 */

	protected boolean validateFileContent(TaskBean task) throws Exception {
		Long auditId = task.getAuditId();
		logger.info("Rejected "+dataBaseServiceAdaptor.updateObject("RejectSGSVinsWithEmptyInspectionDates", auditId)+" VIN's with empty inspection date");
		logger.info("Rejected "+dataBaseServiceAdaptor.updateObject("RejectAtcoDuplicateVins", auditId)+" duplicate VIN's");
		logger.info("Rejected "+dataBaseServiceAdaptor.updateObject("RejectAtcoVinsWithNullInspectionMileage", auditId)+" VIN's with empty inspection mileage");
		logger.info("Rejected "+dataBaseServiceAdaptor.updateObject("RejectAtcoVinsWithNullOdometerCondition", auditId)+" VIN's with empty odometer condition");
		logger.info("Rejected "+dataBaseServiceAdaptor.updateObject("RejectAtcoVinsWithNullFrameDamage", auditId)+" VIN's with empty frame damage");
		List<KeyValuePair> rejectedVins = (List<KeyValuePair>)dataBaseServiceAdaptor.queryList("RejectedAtcoVins", task.getAuditId());
		for(KeyValuePair pair:rejectedVins){
			reportError(pair.getKey(), pair.getValue());
		}
		return true;
	}

	protected List<AtcoStageVehicle> getAllLoadedVins() throws Exception{
		return (List<AtcoStageVehicle>)dataBaseServiceAdaptor.queryList("AllChryslerCapitalLoadedVins", getInfo());
	}

	//Overwrite
    protected List<AtcoStagePicture> getPhotoURLsToDownload(AtcoStageVehicle veh) throws Exception{
    	List<AtcoStagePicture> toDownload = (ArrayList<AtcoStagePicture>)dataBaseServiceAdaptor.queryList("DistinctPhotoURLandCommentsToDownloadWithoutSellerOrgId", veh);
    	return toDownload;
    }

	//Overwrite
	protected boolean isPrimaryPicture(String comments){
		if("Left Front Profile".equalsIgnoreCase(comments)){
			return true;
		}
		return false;
	}

	protected boolean validateVehicle(AtcoStageVehicle veh) throws Exception {

		if(veh.getVin() == null || !veh.getVin().matches("^[a-zA-Z0-9]{17}$")){
			reportError(veh, "Invalid Vin: "+veh.getVin()+". Inspection not loaded.");
			return false;
		}

		//reject if vin in a vehicle status with upd_dmg_and_acct_info != 1

		Long updateDamageAndAccountInfo = (Long)dataBaseServiceAdaptor.queryObject("CCSgsUpdateDamageAndAccountInfoFlag", veh);
		Long dealerInspected=(Long)dataBaseServiceAdaptor.queryObject("CcapDealerInspected", veh);

		if(new Long(1L).equals(dealerInspected)){
			reportError(veh, "VIN's is in Status (2,10) OR Dealer Inspection is Present in GD. The inspection did not get loaded.");
			return false;
		}
		if(new Long(0L).equals(updateDamageAndAccountInfo)){
			reportError(veh, "VIN is in a status in which inspection upload is not allowed. The inspection did not get loaded.");
			return false;
		}
		//VIN not in the system, create it.

		if (veh.getVehicleId() == null) {
			veh.setSystemId(2L);
			veh.setVehicleStatusId(10L);
			veh.setVehicleStatusName(VehicleStatusId.AWAITING_SELLER_DATA.name());
			logger.info(dataBaseServiceAdaptor.addObject("ChryslerCapitalVehicles", veh)
					+ " Vehicle record inserted for vin = "
					+ veh.getVin());
			return true;
		}

		if (veh.getSystemId().equals(SystemId.POST_SALES.getValue())) {
				reportError(veh, "VIN is already in Post Sales system. The inspection did not get loaded.");
				return false;
		}
		if (SystemId.SALES.getValue().equals(veh.getSystemId()) && VehicleStatusId.AUCTION.getValue().equals(veh.getVehicleStatusId())){

			Long highBidId = (Long)dataBaseServiceAdaptor.queryObject("HighBidId", veh.getVehicleId());

				if(highBidId != null){
					reportError(veh, "VIN is currently in auction with some bids. The inspection did not get loaded");
					return false;
				}
			}

		// 5.21 Seller removed vehicle
		if (veh.getSystemId().equals(SystemId.INACTIVATED_VEHICLES.getValue())	&& veh.getVehicleStatusId().equals(VehicleStatusId.SELLER_REMOVED_VEHICLE.getValue())) {
					reportBuffer.append(veh.getVin()+ ", VIN is currently removed by Seller. Record  did not get loaded\n");
					veh.setExcpTypeReason("VIN is currently in No Sale");
					logger.warn("VIN is currently removed by Seller. Record  did not get loaded:"+veh.getVin());
					return false;
				}
		return true;
	}

	protected boolean identifySeller(TaskBean task) {
		return true;  // Do nothing.  getSellerOrgId() overridden with static value
	}

	protected boolean setInspectionOrgId(TaskBean task){
		AtcoStageVehicle asv = new AtcoStageVehicle();
		asv.setSellerOrganizationId(getSellerOrgId());
		asv.setInspectionOrgId(getInspectionOrgId());
		asv.setDbProcessId(task.getAuditId());
		setInfo(asv);
		return true;
	}
	//Overwrite
	protected void postTransaction(AtcoStageVehicle veh) throws Exception{
		logger.info("Calling LifeCycle Manager");
		chryslerVehicleLifeCycleManager.updateVehicleStatus(veh.getVehicleId(),"INSPECTION");
	}
	//Overwrite
	protected void evaluateAuctionRules (AtcoStageVehicle veh) throws Exception {}

	protected void updateAdditionalVehicleInfo(AtcoStageVehicle veh) throws Exception {
		String vin = veh.getVin();
		Long records = null;
		//PPM-93629
		//records = (long)dataBaseServiceAdaptor.updateObject("VehicleCustomParts", veh);
		//logger.info("Inserting into VEHICLES_CUSTOM_PARTS table (" + (records > 0 ? records : 0) + ") for VIN: " + veh.getVin());

		// PPM 98161 Chrysler Capital -Discontinue having SGS inspectoin override make, model and trim
		if(veh.getCarGroupConfigId().equals(new Long(1123))){
			records = (long)dataBaseServiceAdaptor.updateObject("CCAPVehiclesAttributesNotMakeModelSeries", veh) ;
			Double maxMileage = (Double)dataBaseServiceAdaptor.queryObject("CcapMaxMileageValue","CCAP_MAX_INSPECTION_MILEAGE");
			logger.info("InspectionMileage is :"+veh.getInspectionMileage());
			if(veh.getInspectionMileage()>=maxMileage) {
				long a;
				a=dataBaseServiceAdaptor.updateObject("CcapVehicleToSellerRemoved",veh);
				logger.info("Vin : " + veh.getVin() + " Moved to Seller Removed due to Mileage greater than 1,00,000.Rows updated :"+a);
				a=dataBaseServiceAdaptor.updateObject("CcapAuctionToCloseForMaxMileage",veh);
				logger.info("Closing the auction for vehicle vin :"+veh.getVin()+",,,,"+veh.getVehicleId()+"Rows updated :"+a);
			}

		}else{
			records = (long)dataBaseServiceAdaptor.updateObject("CCAPVehiclesAttributes", veh) ;
		}


		logger.info("Updating VEHICLES table attributes (" + (records > 0 ? records : 0) + ") for VIN: " + vin);
		//records = (long)dataBaseServiceAdaptor.updateObject("VehiclesParts", veh);
		//logger.info("Inserting into VEHICLES_PARTS table (" + (records > 0 ? records : 0) + ") for VIN: " + vin);


		//PPM-118970 CCAP: Inspection Mileage Not Updating on the VDP (Ref PPM 117708)
		// Overwrite VEHICLES.Mileage if it is greater than TURN_IN Mileage date
				logger.info("Updating Inspection Mileage to VEHICLES table if inspection Date is greater than TURN_IN date ("
						+ dataBaseServiceAdaptor.updateObject("ChryslerInspectionMileage", veh)
						+ ") in VEHICLES table");
	}

	protected void processVehicle(AtcoStageVehicle veh) throws Exception {
		super.processVehicle(veh);
		Long records = (long)dataBaseServiceAdaptor.updateObject("HondaRepoVehicleTypeId", veh);
		logger.info("Updating OVERALL_CONDITION_ID (" + (records > 0 ? records : 0) + ") old inspection for VIN: " + veh.getVin());
	}

	protected String getProcessTitle() {
		return processTitle;
	}
	protected String getSellerRecipient() {
		return sellerRecipient;
	}
	protected Long getSellerOrgId() {
		return sellerOrgId;
	}
	protected Long getInspectionOrgId() {
		return inspectionOrgId;
	}
	protected AtcoStageVehicle getInfo() {
		return info;
	}
	protected void setInfo(AtcoStageVehicle info) {
		this.info = info;
	}

	public ChryslerVehicleLifeCycleManager getChryslerVehicleLifeCycleManager() {
		return chryslerVehicleLifeCycleManager;
	}

	public void setChryslerVehicleLifeCycleManager(
			ChryslerVehicleLifeCycleManager chryslerVehicleLifeCycleManager) {
		this.chryslerVehicleLifeCycleManager = chryslerVehicleLifeCycleManager;
	}

	//PPM-114278 CCAP- CG 1123- Inspections were not applied to attached VINs
	protected boolean deleteAllStagingTables() throws Exception {
		AtcoStageVehicle asv = getInfo();
		try {
			dataBaseServiceAdaptor.startTransaction();
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageVehicles", asv)
					+ " AtcoStageVehicles records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageInspections", asv)
					+ " AtcoStageInspections records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageTires", asv)
					+ " AtcoStageTires records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageDamages", asv)
					+ " AtcoStageDamages records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageParts", asv)
					+ " AtcoStageParts records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStagePictures", asv)
					+ " AtcoStagePictures records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			logger.info(dataBaseServiceAdaptor.deleteObject("CleanAtcoStageLocations", asv)
					+ " AtcoStageLocations records deleted for sellerOrgId = " + asv.getSellerOrganizationId()
					+ " and inspectionOrgId=" + asv.getInspectionOrgId());
			dataBaseServiceAdaptor.commitTransaction();
		} catch (Exception e) {
			logger.error("Something crash during deleting all the staging tables");
			logger.fatal(e);
			sendErrorDocument(getOperationsRecipient(), null, null, "DS2K7 - " + getProcessTitle() + " Upload Failed!",
					"Something crash during deleting all the staging tables, please check the log file for details.");
			return false;

		} finally {
			dataBaseServiceAdaptor.endTransaction();
		}
		return true;

	}

}


using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VCDM2.Data.EF.OrcanIntelligence;

namespace VCDM2.Data.EF
{
    public static class EFProvider
    {
        public static bool Import(DataTable dataTable)
        {
            try
            {
                bool boolVal;
                DateTime dateTimeVal;
                using (var db = new OIEntities())
                {
                    foreach (DataRow row in dataTable.Rows)
                    {
                        var contact = new Contact();

                        contact.Id = row["Id"].ToString();
                        if(bool.TryParse(row["IsDeleted"].ToString(), out boolVal))
                        {
                            contact.IsDeleted = boolVal;
                        }
                        contact.MasterRecordId = row["MasterRecordId"].ToString();
                        contact.AccountId = row["AccountId"].ToString();
                        contact.Salutation = row["Salutation"].ToString();
                        contact.FirstName = row["FirstName"].ToString();
                        contact.LastName = row["LastName"].ToString();
                        contact.RecordTypeId = row["RecordTypeId"].ToString();
                        contact.OtherStreet = row["OtherStreet"].ToString();
                        contact.OtherCity = row["OtherCity"].ToString();
                        contact.OtherState = row["OtherState"].ToString();
                        contact.OtherPostalCode = row["OtherPostalCode"].ToString();
                        contact.OtherCountry = row["OtherCountry"].ToString();
                        contact.OtherLatitude = row["OtherLatitude"].ToString();
                        contact.OtherLongitude = row["OtherLongitude"].ToString();
                        contact.OtherGeocodeAccuracy = row["OtherGeocodeAccuracy"].ToString();
                        contact.MailingStreet = row["MailingStreet"].ToString();
                        contact.MailingCity = row["MailingCity"].ToString();
                        contact.MailingState = row["MailingState"].ToString();
                        contact.MailingPostalCode = row["MailingPostalCode"].ToString();
                        contact.MailingCountry = row["MailingCountry"].ToString();
                        contact.MailingLatitude = row["MailingLatitude"].ToString();
                        contact.MailingLongitude = row["MailingLongitude"].ToString();
                        contact.MailingGeocodeAccuracy = row["MailingGeocodeAccuracy"].ToString();
                        contact.Phone = row["Phone"].ToString();
                        contact.Fax = row["Fax"].ToString();
                        contact.MobilePhone = row["MobilePhone"].ToString();
                        contact.HomePhone = row["HomePhone"].ToString();
                        contact.OtherPhone = row["OtherPhone"].ToString();
                        contact.AssistantPhone = row["AssistantPhone"].ToString();
                        contact.ReportsToId = row["ReportsToId"].ToString();
                        contact.Email = row["Email"].ToString();
                        contact.Title = row["Title"].ToString();
                        contact.Department = row["Department"].ToString();
                        contact.AssistantName = row["AssistantName"].ToString();
                        contact.LeadSource = row["LeadSource"].ToString();
                        contact.Birthdate = row["Birthdate"].ToString();
                        contact.Description = row["Description"].ToString();
                        contact.OwnerId = row["OwnerId"].ToString();
                        if (bool.TryParse(row["HasOptedOutOfEmail"].ToString(), out boolVal))
                        {
                            contact.HasOptedOutOfEmail = boolVal;
                        }
                        if (bool.TryParse(row["HasOptedOutOfFax"].ToString(), out boolVal))
                        {
                            contact.HasOptedOutOfFax = boolVal;
                        }
                        if (bool.TryParse(row["DoNotCall"].ToString(), out boolVal))
                        {
                            contact.DoNotCall = boolVal;
                        }
                        if (DateTime.TryParse(row["CreatedDate"].ToString(), out dateTimeVal))
                        {
                            contact.CreatedDate = dateTimeVal;
                        }
                        contact.CreatedById = row["CreatedById"].ToString();
                        if (DateTime.TryParse(row["LastModifiedDate"].ToString(), out dateTimeVal))
                        {
                            contact.LastModifiedDate = dateTimeVal;
                        }
                        contact.LastModifiedById = row["LastModifiedById"].ToString();
                        if (DateTime.TryParse(row["SystemModstamp"].ToString(), out dateTimeVal))
                        {
                            contact.SystemModstamp = dateTimeVal;
                        }
                        if (DateTime.TryParse(row["LastActivityDate"].ToString(), out dateTimeVal))
                        {
                            contact.LastActivityDate = dateTimeVal;
                        }
                        contact.LastCURequestDate = row["LastCURequestDate"].ToString();
                        contact.LastCUUpdateDate = row["LastCUUpdateDate"].ToString();
                        contact.EmailBouncedReason = row["AccountId"].ToString();
                        if (DateTime.TryParse(row["EmailBouncedDate"].ToString(), out dateTimeVal))
                        {
                            contact.EmailBouncedDate = dateTimeVal;
                        }
                        contact.Jigsaw = row["Jigsaw"].ToString();
                        contact.JigsawContactId = row["AccountId"].ToString();
                        contact.AVTRRT__AutoPopulate_Skillset__c = row["AccountId"].ToString();
                        contact.AVTRRT__Availability_To_Interview__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Available_To_Start__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Background_Check__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Birthday__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Candidate_Id__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Candidate_Short_List__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Candidate_Status__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Candidate_Summary__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Candidate_Write_Up__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Cell_Phone__c = row["AccountId"].ToString();
                        //contact.AVTRRT__City__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Confirmation__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Country__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Cover_Letter__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Current_Pay__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Department__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Desired_Pay_Range_From__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Education_CandidateProf__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Education_Details__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Ethnicity__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Experience__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Fax__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Gender__c = row["AccountId"].ToString();
                        //contact.AVTRRT__General_Competency__c = row["AccountId"].ToString();
                        //contact.AVTRRT__General_Job_Category_Sought__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Home_Email__c = row["AccountId"].ToString();
                        //contact.AVTRRT__IT_Competency__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Internal_Candidate__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Job_Title__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Jobs_Notified__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Languages__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Locations_1st_Choice__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Notes_and_Comments__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Notify_Candidate__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Online_Profile_Link__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Open_For_Relocation__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Other_Competency__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Other_Emails__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Pager__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Parse_Resume_Migration_Id__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Pay_Type__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Percentage_of_Travel__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Phone__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Postal_Code__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Preferred_Job_Term__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Previous_Employers__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Previous_Titles__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Recruiter__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Referral_Date__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Referral_Relationship__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Referral_Source__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Resume_Received_Date__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Skill_Matched__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Source__c = row["AccountId"].ToString();
                        //contact.AVTRRT__State__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Street_Address__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Time_Zone__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Veteran_Status__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Video_Resume_Link__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Status__c = row["AccountId"].ToString();
                        //contact.AVTRRT__X2nd_Choice__c = row["AccountId"].ToString();
                        //contact.AVTRRT__isRequired__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Jobs_Applied__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Jobs_Notified_Long__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Update_Skills__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Duplicate_Criteria__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Is_Duplicate__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Resume_Attachment_Id__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Chatter_Id__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Employment_History_Org_Name__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Jobs__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Migration_Id__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Recent_Template__c = row["AccountId"].ToString();
                        //contact.AVTRRT__ResumeRich__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Resume_Link__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Lat__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Lon__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Map_Which_Address__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Mapping_Status__c = row["AccountId"].ToString();
                        //contact.FCMS__CMSProfile__c = row["AccountId"].ToString();
                        //contact.FCMS__Company_Name__c = row["AccountId"].ToString();
                        //contact.FCMS__Password__c = row["AccountId"].ToString();
                        //contact.FCMS__PortalEmailAlert__c = row["AccountId"].ToString();
                        //contact.FCMS__Profile_Manager_Email__c = row["AccountId"].ToString();
                        //contact.FCMS__Registration_Approved__c = row["AccountId"].ToString();
                        //contact.FCMS__UserName__c = row["AccountId"].ToString();
                        //contact.JOBBS__Daxtra_Candidate_Id__c = row["AccountId"].ToString();
                        //contact.JOBBS__Job_Board_ProfileId__c = row["AccountId"].ToString();
                        //contact.AVTRRT__CallEmAll_Broadcast_Id__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Current_Employer__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Driver_Licence_Number__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Driver_Licence_State__c = row["AccountId"].ToString();
                        //contact.AVTRRT__New_Email__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Prophecy_Caregiver_ID__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Resume_Name__c = row["AccountId"].ToString();
                        //contact.AVTRRT__SSN__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Total_Experience_Months__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Total_Experience_Years__c = row["AccountId"].ToString();
                        //contact.AVTRRT__External_ID__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Updated_Account_from_Current_Employer__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_10__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_11__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_12__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_13__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_14__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_1__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_2__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_3__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_4__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_5__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_6__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_7__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_8__c = row["AccountId"].ToString();
                        //contact.AVTRRT__Work_Details_History_9__c = row["AccountId"].ToString();
                        //contact.FCMS__Job_Site__c = row["AccountId"].ToString();
                        //contact.FCMS__LinkedInId__c = row["AccountId"].ToString();
                        //contact.TRGTFCMS__Client_Portal_Super_User__c = row["AccountId"].ToString();
                        //contact.TRGTFCMS__Publish__c = row["AccountId"].ToString();
                        //contact.TRGTFCMS__Vendor_Contact__c = row["AccountId"].ToString();
                        //contact.TRGTFCMS__Vendor_Notes__c = row["AccountId"].ToString();
                        //contact.TRGTFCMS__Vendor__c = row["AccountId"].ToString();
                        //contact.FCMS__FacebookId__c = row["AccountId"].ToString();
                        //contact.FCMS__GoogleId__c = row["AccountId"].ToString();
                        //contact.Consent__c = row["AccountId"].ToString();
                        //contact.Date_of_Consent__c = row["AccountId"].ToString();
                        //contact.Privacy_consent__c = row["AccountId"].ToString();
                        //contact.Email_consent__c = row["AccountId"].ToString();

                        db.Contacts.Add(contact);
                    }

                    db.SaveChanges();

                    return true;
                }
            }
            catch(Exception e)
            {
                return false;
            }
        }
    }
}

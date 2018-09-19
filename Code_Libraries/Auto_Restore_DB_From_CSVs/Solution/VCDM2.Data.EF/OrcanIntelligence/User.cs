//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace VCDM2.Data.EF.OrcanIntelligence
{
    using System;
    using System.Collections.Generic;
    
    public partial class User
    {
        public string Id { get; set; }
        public string Username { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string CompanyName { get; set; }
        public string Division { get; set; }
        public string Department { get; set; }
        public string Title { get; set; }
        public string Street { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }
        public string Latitude { get; set; }
        public string Longitude { get; set; }
        public string GeocodeAccuracy { get; set; }
        public string Email { get; set; }
        public string SenderEmail { get; set; }
        public string SenderName { get; set; }
        public string Signature { get; set; }
        public string StayInTouchSubject { get; set; }
        public string StayInTouchSignature { get; set; }
        public string StayInTouchNote { get; set; }
        public Nullable<int> Phone { get; set; }
        public Nullable<int> Fax { get; set; }
        public string MobilePhone { get; set; }
        public string Alias { get; set; }
        public string CommunityNickname { get; set; }
        public Nullable<bool> IsActive { get; set; }
        public Nullable<bool> IsSystemControlled { get; set; }
        public string TimeZoneSidKey { get; set; }
        public string UserRoleId { get; set; }
        public string LocaleSidKey { get; set; }
        public Nullable<bool> ReceivesInfoEmails { get; set; }
        public Nullable<bool> ReceivesAdminInfoEmails { get; set; }
        public string EmailEncodingKey { get; set; }
        public string ProfileId { get; set; }
        public string UserType { get; set; }
        public string UserSubtype { get; set; }
        public Nullable<short> StartDay { get; set; }
        public Nullable<short> EndDay { get; set; }
        public string LanguageLocaleKey { get; set; }
        public string EmployeeNumber { get; set; }
        public string DelegatedApproverId { get; set; }
        public string ManagerId { get; set; }
        public Nullable<System.DateTime> LastLoginDate { get; set; }
        public Nullable<System.DateTime> LastPasswordChangeDate { get; set; }
        public Nullable<System.DateTime> CreatedDate { get; set; }
        public string CreatedById { get; set; }
        public Nullable<System.DateTime> LastModifiedDate { get; set; }
        public string LastModifiedById { get; set; }
        public Nullable<System.DateTime> SystemModstamp { get; set; }
        public Nullable<System.DateTime> SuAccessExpirationDate { get; set; }
        public Nullable<System.DateTime> SuOrgAdminExpirationDate { get; set; }
        public string OfflineTrialExpirationDate { get; set; }
        public string WirelessTrialExpirationDate { get; set; }
        public string OfflinePdaTrialExpirationDate { get; set; }
        public Nullable<bool> ForecastEnabled { get; set; }
        public string ContactId { get; set; }
        public string AccountId { get; set; }
        public string CallCenterId { get; set; }
        public string Extension { get; set; }
        public string FederationIdentifier { get; set; }
        public string AboutMe { get; set; }
        public string LoginLimit { get; set; }
        public string ProfilePhotoId { get; set; }
        public string DigestFrequency { get; set; }
        public string DefaultGroupNotificationFrequency { get; set; }
        public string WorkspaceId { get; set; }
        public string SharingType { get; set; }
        public string ChatterAdoptionStage { get; set; }
        public Nullable<System.DateTime> ChatterAdoptionStageModifiedDate { get; set; }
        public string BannerPhotoId { get; set; }
        public Nullable<bool> IsProfilePhotoActive { get; set; }
        public string AVTRRT__Candidate_Pool_Account_Name__c { get; set; }
    }
}

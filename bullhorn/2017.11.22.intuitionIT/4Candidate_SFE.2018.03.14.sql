
with 
SkillName as (
       SELECT [BH_Candidate].[candidateID]
             ,[BH_Candidate].[userID]
             ,[BH_Candidate].[type]
             ,[BH_Candidate].[status]
             ,[BH_Candidate].[dateAdded]
                ,[BH_User].name as uname
                ,[BH_UserContact].name as usname
                ,[BH_UserSkill].skillID
                ,[BH_SkillList].name as SkillName
         FROM [bullhorn1].[BH_Candidate]
         LEFT JOIN [bullhorn1].[BH_User] on [BH_User].userID = [BH_Candidate].userID
         LEFT JOIN [bullhorn1].BH_UserContact on BH_User.userID = BH_UserContact.userID
         LEFT JOIN [bullhorn1].BH_UserSkill on BH_Candidate.userID = BH_UserSkill.userID
         LEFT JOIN [bullhorn1].BH_SkillList on BH_UserSkill.skillID = BH_SkillList.skillID
         --where [bullhorn1].[BH_Candidate].isPrimaryOwner = 1
 )
--select distinct SkillName from SkillName where SkillName not in ('$Univers','.NET','1C','4G','AA','ABAP','Abroad','Absence','Accountant','Acd','ActionScript','Active Directory','Active Enterprise','Active Matrix','ActiveX','Actuary','Actuate','Adapter SDK','ADI','Administer Training','Administer Workforce','Administrator','Adobe','Adonix','Adoption','ADP','ADSL','Advanced Collections','Advanced Configurator','Advanced Planning','AFF','Afrikaans','AFS','Agile','AIX','Ajax','ALE','Alfresco','AM','Amazon Web Services','AME','AMF','AMOA','AMX BPM','Analytics','Android','angular js','AP','Apache','API','APO','Application designer','Application developer','Application engine','Application Messaging','Application Packages','Application server','Approval Management','APS','Aptitude','AQ','Aqualogic','AquaLogic Architect','AquaLogic BPM','AquaLogic ESB','AR','Arabic','Arbor','Architect','ARIBA','Art Director','AS/400','AS3','ASCP','ASP.NET','ATM','AX','AX2009','AX2012','AX4','Axapta','Axure','Azure AAD','B2B Sales/Recruitment Consultant','BACKBONE','Back-End Developer','Bale','BAM','Bank Reconciliation','Banking Consultant','BAPI','Bash','Basis','BC','BCS','Benefits','BI','BI Architect','bi publisher','BIA','Big Data','Big Data Analyst','Big Data Architect','Big Data Chief Officer','Big Data Developer','BIG DATA Ecole Gr.1','Big Data Engineer','Big Data Integration Specialist','Big Data Miner','BIG DATA Scientist','Big Data Visualizer','BI-IP','Billing','BizTalk 2009','Biztalk 2010','BizTalk Architect','BizTalk Business Analyst','Biztalk Developer','BizTalk Functional Consultant','BizTalk Systems Administrator','BO','BODI','BOM','Bootstrap','BPM','BPR','BPS','Brio','BRM','Bscs','BSS Consultant','BSS Solution Architect','Bulgarian','Business Analyst','Business Intelligence','Business Intelligence Architect','Business Intelligence Business Analyst','Business Intelligence Developer','Business Intelligence Functional Analyst','Business Intelligence Project Director','Business Intelligence Project Manager','Business Intelligence Techno Functional Consultant','Business Intelligence Trainer','Business Process Consultant','Business Rules','Business Transformation','BusinessConnect','BusinessEvents','BusinessStudio','BusinessWorks','BusinessWorks 2nd','BusinessWorks 6','BW','C','C#','C++','Ca','Campus Solutions','Candidate Gateway','Cantonese','Capital Market Consultant','Captiva','Cassandra','CC','CCNA/CCNP','CDMA/WCDMA','Centre','Change management','Charging Systems','Checkpoint','Chinese','Chronos','CI','Cim','Cisco','CITRIX','Clementine','Clojure','cloud','Cloud Architect','Cloud Development','Cloud Security','cloudera','CM','CMS','CO','Coach Agile','COBOL','CODA','Cognos','Cognos TM1','Cold Fusion','Collaborator','Compensation','Compliance Consultant','COMPONENT INTERFACE','Configuration Manager','Configurator','Consultancy CRM','Consulting','CONTROL M','Copywriting','CORBA','Cordova','Core Networks','Cornerstone','CouchDB','CRM','Croatian','CROSS-PLATFORM','CrossWorlds','Crystal','CS','CSS','CTO','Customer Service CRM Module','Customer Success Manager','Cyber Security','Czech','d3js','DADS-U','Danish','Data Analysis','Data Architecture','Data Centre','Data Cleansing','DATA INTEGRATION','Data Migration','Data mining','Data Modelling','Data Quality','data science','Data Warehousing','Database Administrator','Database Architect','Database Design','Database Development','DATAMART','Datamining','DataStage','DB2','Debian','Dell Quest','Dell Quest One','Delphi','Demandware','Developer','DevOps','DHCP','Digital Payment Consultant','Digital PM','Director of Marketing and Communications','Discoverer','django','DM','DNS','Documentum','DOS','DP','Drupal','DS','Dutch','Dynamics','Dynamics Functional','Dynamics Technical','Dynasight','EAI','EAM','EBTAX','ECM','Ecommerce','e-commerce','E-commerce Development','eCommunication','eCompensation','ECONFIGURATOR','eDevelopment','EDI','eEnergy','EH&S','EIM','ElasticSearch','Electronic Money','ELM','eMarketing','EmberJs','Ems','Engineer','English','ePerformance','epharma','EPM','eProcurement','eProfile','eRecruitment','Erlang','ESA','eSales','eSettlement','eSourcing','ESSBase','Est','ESX','Ethernet','ETL','Etraining','Ets','EX','Exchange','Ext JS','EZpublish','F&R','FA','FAH','FI','Fi-co','Filenet','Finance AX Module','Finance Business Transformation','Finance NAV Module','Financials','Finnish','Fintech','fiori','Firewall','Flash','Flume','FM','Foodtech','Forms','French','Front-End Developer','FS-CD','FSCM','FS-CM','FS-CS','FS-PM','Full Stack','Functional','Functional Architect','Functional AX','Functional CRM','Functional NAV','Fusion','Fusion HR Consultant','GC','Genesys','Genio','German','GI','GIT','Gl','Global Payroll','Golang','Google Apps','Google Cloud Platform','Governance','Gprs','GRC','Greek','Groovy','GSM','GTA','Hadoop','Hal','HANA','Hbase','Hebrew','Helpdesk','Hibernate','Hitachi','Hive','hortonworks','Hotonworks','HP Cloud','HPQC','HP-UX','HR','HR ACCESS','HR Access Functional','HR Access Technical','HR AX Module','HR Business Transformation','HRIS Project Manager','HTML','HUM','Hungarian','HYBRID','Hybride Cloud','Hybris','Hyperion','IAM','IBM BPM','IBM cloud computing','IBM Identity Manager','IBM MDM','IDAM','Identity Access Management','IDOC','IDS','IEXPENSES','IHM','Ile de France','iLearning','Indian','Infor M3','Informatica','Infrastructure','Integration Broker','Interfacing','Interim Management','INTERSHOP','INV','ios developer','IOT','IP','iProcess','iProcurement','IRANIAN','iRecruitment','IS','IS Pharma','ISAM','ISAMESSO','IS-Auto','ISDS','IS-FS','IS-H','IS-Media','IS-Mill','IS-OIL','IS-RE','IS-Retail','IS-U','IT Security Recruitment Consultant','Italian','ITIL','J2EE','Jahia','Japanese','Java','Java Architect','Java Developer','JAVA FX','Java lead dev','Java Project Manager','Javascript','JD EDWARDS','JDEDWARDS','Jira','Joomla!','jQuery','JSON','JSP','Juniper','Junit','Kafka','Kanban','KAZAKH','Kshell','L4G','LAN','Laravel','Latvian','LE','Lean','Lease Management','Linux','Lithuanian','Logistics','Lotus Notes','Loyalty','LSMW','LSO','LTE','MAC','Machine Learning','Magento','Managing Director','Manufacturing','Manufacturing AX Module','Manufacturing NAV Module','MapReduce','Marketing CRM Module','Matlab','Maven','MDM','MDX','MEAN Stack','Mega Data','Mercury','Message Broker','Meteor','Meteor JS','Microgen','Microsoft Azure','Microsoft BI','Microsoft CRM','Microsoft CRM BA','Microsoft CRM Developer','Microsoft CRM Director','Microsoft CRM PM','Microsoft Dynamics AX','Microsoft Dynamics AX Developer','Microsoft Dynamics AX Finance','Microsoft Dynamics AX Functional Consultant','Microsoft Dynamics AX PM','Microsoft Dynamics AX Pre-Sales','Microsoft Dynamics AX Sales','Microsoft Dynamics AX Solutions Architect','Microsoft Dynamics AX Support','Microsoft Dynamics AX Technical Consultant','Microsoft Dynamics AX Test Analyst','Microsoft Dynamics CRM','Microsoft Dynamics CRM PM','microsoft dynamics nav','Microsoft Dynamics NAV BA','Microsoft Dynamics NAV Developer','Microsoft Dynamics NAV Director','Microsoft Dynamics NAV Functional','Microsoft Dynamics NAV PM','Microsoft Dynamics NAV pre-sales','Microsoft Dynamics NAV Sales','Microsoft Dynamics NAV Support','Microsoft Dynamics NAV Trainer','MicroStrategy','Middle East','MM','Mobile Development','MongoDB','Movex','MQ Series','MRO','MRP','MSCA','MUREX ARCHITECT','MySQL','NAS','Netbackup','NETSUITE','Netweaver','Netweaver Gateway','Network administrator','Network engineer','Networker','Noc','Node JS','NORD','norweigan','NoSQL','Nvision','OAF','OAM','OBIEE','Objective-C','Ocs','oData','ODI','ODS','OFA','OIC','OIL HPM','OIL SSR','OIL TD','OIL TSW','OIM','OKE','OKS','OLAP','OLM','OM','ONDEMAND','OO Programming','OPEN','OpenStack','OPENTEXT','OPM','optimization','Oracle','Oracle Administrator','Oracle Database Developer','Oracle EBS','Oracle EBS Administrator','Oracle EBS CRM Business Analyst','Oracle EBS CRM Functional Consultant','Oracle EBS CRM Project Director','Oracle EBS CRM Project Manager','Oracle EBS CRM Support Consultant','Oracle EBS CRM Technical Analyst','Oracle EBS Financials Business Analyst','Oracle EBS Financials Functional Consultant','Oracle EBS Financials Project Director','Oracle EBS Financials Project Manager','Oracle EBS Financials Support Consultant','Oracle EBS Financials Technical Analyst','Oracle EBS HRMS Business Analyst','Oracle EBS HRMS Functional Consultant','Oracle EBS HRMS Project Director','Oracle EBS HRMS Project Manager','Oracle EBS HRMS Support Consultant','Oracle EBS HRMS Technical Analyst','Oracle EBS HRMS Techno Functional','Oracle EBS Logistics Business Analyst','Oracle EBS Logistics Functional Consultant','Oracle EBS Logistics Project Director','Oracle EBS Logistics Project Manager','Oracle EBS Logistics Support Consultant','Oracle EBS Logistics Techno Functional','Oracle EBS Trainer','Oracle Fusion','Oracle Fusion Financials','Oracle Fusion HCM Functional Consultant','Oracle Fusion HCM Technical Consultant','Oracle Fusion Middleware','Oracle Fusion PPM','Oracle Fusion Procurement','Oracle Identity Management','Oracle SOA','Oracle SOA Architect','Oracle SOA Developer','Oracle SOA Project Manager','Oracle SOA Systems Administrator','Oracle SOA Tester','Order Capture','Order Management','OSS Consultant','OSS Solution Architect','OST','OTA','OTC','Other','OTL','OTM','Ouest','PA','PAB','PAC','Pascal','Payment Consultant','Payment Processing','Payroll','PC','Pega Architect','Pega CBA','Pega CCA','Pega CLSA','Pega CSA','Pega CSSA','PEGA Developer','PEGA LSA','Pega PRPC','Pentaho','Penthao','Peoplecode','PeopleSoft','Peoplesoft Administrator','Peoplesoft Architect','PeopleSoft Campus Developer','PeopleSoft Campus Functional','PeopleSoft Campus Project Manager','PeopleSoft Campus Techno Functional','PeopleSoft CRM Developer','PeopleSoft CRM Functional','PeopleSoft CRM Project Manager','PeopleSoft CRM Techno Functional','PeopleSoft EPM Developer','PeopleSoft EPM Functional','PeopleSoft EPM Project Manager','PeopleSoft EPM Techno Functional','PeopleSoft ESA Developer','PeopleSoft ESA Functional','PeopleSoft ESA Project Manager','PeopleSoft ESA Techno Functional','PeopleSoft Financials Developer','PeopleSoft Financials Functional','PeopleSoft Financials Project Manager','PeopleSoft Financials Techno Functional','PeopleSoft FSCM Developer','PeopleSoft FSCM Functional','PeopleSoft FSCM Project Manager','PeopleSoft FSCM Techno Functional','PeopleSoft HR Developer','PeopleSoft HR Functional','PeopleSoft HR Project Manager','PeopleSoft HR Techno Functional','PeopleSoft Payroll Developer','PeopleSoft Payroll Functional','PeopleSoft Payroll Project Manager','PeopleSoft Payroll Techno Functional','PeopleSoft Project Director','Peoplesoft Tester','PEOPLESOFT TRAINER','PeopleTools','PERL','Persian','phonegap','PHP','PHP lead dev','PI','PIG','PL/SQL','Play Framework','PLM','PM','PM/Program Manager AX','PM/Program Manager CRM','PM/Program Manager NAV','PMO','Po','Policy Manager','Polish','Portal','Portuguese','POS DM','Post Merger Integration Consultant','Post Production Engineer','Postgresql','postsales','PowerShell','PP','Predictive Analytics','Pre-Production Engineer','Presales','Prestashop','Pretashop','Pricing','Private Cloud','ProcessServer','Procurement','Procurement AX Module','PRODUCT CATALOGUE','Product Director','Product Manager','Product Owner','Production Analyst','Programme Director','Project Manager','Prolog','PS','PS-Query','Public Cloud','Purchasing AX Module','Python','QlikView','QM','QP','QTP','Quality Engineer','Quality Management','QualityCenter','quant','R','R&D','R.','RAN','RDJ','React JS','ReactJS','Recruitment','Recruitment Resourcer','Redhat','Remedy','Rendezvous','Reports','Requirement Engineering','Responsive Design','Responsive Development','RESTFUL','Retail','Retail AX Module','Risk Backtesting','Risk Compliance Consultant','Risk Credit','Risk EAD','Risk IFRS9','Risk LGD','Risk Management','Risk market frtb','Risk market quant','Risk market regulatory','Risk MOA','Risk PD','Risk Score Carding','Risk Stress Testing','RM','RMCA','Romanian','RPG','Ruby','Ruby on Rails','Russian','SA','SAAS','SAGE','Sage 100','Sage 1000','Sage CRM','Sage Paie','Sage x3','Sales','Sales and Marketing NAV Module','Sales AX Module','Sales CRM Module','Sales Director','Salesforce','Salesforce.com Administrator','Salesforce.com Architect','Salesforce.com Functional','Salesforce.com Project Manager','Salesforce.com Technical','SAP','SAP ABAP Developer','SAP Adapter','SAP Admin','SAP AMOA Finance Consultant','SAP AMOA Logistic Consultant','SAP APO CONSULTANT','SAP APO Techno/Fun Consultant','SAP Bank Analyzer Consultant','SAP Bank Analyzer Techno/Fun Consultant','SAP BASIS Administrator/Consultant','SAP BCA Consultant','SAP BCA Techno/Fun Consultant','SAP BCS Consultant','SAP BCS Techno/Fun Consultant','SAP BFC Consultant','SAP BI Consultant','SAP BI Techno-Functional Consultant','SAP BI-IP Consultant','SAP BI-IP Techno/Fun Consultant','SAP BO Administrator','SAP BO Consultant','SAP BO Techno/Fun Consultant','SAP BPC Consultant','SAP BRF','SAP BRIM Consultant','Sap Business Analyst','SAP BW Consultant','Sap Bw Developer','SAP BW Techno/Fun Consultant','SAP CFM Consultant','SAP CFM Techno/Fun Consultant','SAP CML Consultant','SAP CML Techno/Fun Consultant','SAP CO Consultant','SAP CO Techno/Fun Consultant','SAP Core Banking Consultant','SAP Core Banking Techno/Fun Consultant','Sap Crm Consultant','SAP CRM Developer','SAP CS Consultant','SAP CS Techno/Fun Consultant','SAP CTRM Consultant','SAP CTRM Techno/Functional Consultant','SAP DATA MIGRATION CONSULTANT','SAP DM Consultant','SAP DM Techno/Fun Consultant','SAP EDI Techno/Fun Consultant','SAP EHS Consultant','SAP EM Consultant','SAP EWM Consultant','SAP EWM Techno/Fun Consultant','Sap Fi Consultant','SAP FI Techno/Fun Consultant','SAP FI-AA Consultant','SAP FI-AA Techno/Functional Consultant','SAP FI-CA Consultant','SAP Financials Business Analyst','SAP Fiori','SAP FM Consultant','SAP FM Techno/Fun Consultant','SAP FS-BP Consultant','SAP FS-CD Consultant','SAP FS-CM Consultant','SAP FS-CS Consultant','SAP FS-ICM Consultant','SAP FS-PM Consultant','SAP FS-RI Consultant','SAP GRC Consultant','SAP GRC Techno/Fun Consultant','SAP GTS Consultant','SAP GTS Techno/Fun Consultant','SAP HANA Consultant','SAP HCM Business Analyst','SAP HCM Consultant','SAP HCM Developer','SAP HCM Techno/Fun Consultant','SAP HR','SAP HUM Consultant','SAP IPM','SAP IS-A&D Consultant','SAP IS-AFS Consultant','SAP IS-AFS Techno/Fun Consultant','SAP IS-Auto Consultant','SAP IS-Auto Techno/Fun Consultant','SAP IS-Bev Consultant','SAP IS-EH&S Consultant','SAP IS-EH&S Techno/Fun Consultant','SAP IS-H Consultant','SAP IS-H Techno/Fun Consultant','SAP IS-Media Consultant','SAP IS-Media Techno/Fun Consultant','SAP IS-Mill Consultant','SAP IS-OIL CONSULTANT','SAP IS-Oil Techno/Fun Consultant','SAP IS-Procurement Techno/Functional Consultant','SAP IS-PS Consultant','SAP IS-RE Consultant','SAP IS-Retail Consultant','SAP IS-Retail Techno/Fun Consultant','SAP IS-T Consultant','SAP IS-T Techno/Fun Consultant','SAP IS-U CONSULTANT','SAP IS-U Techno/Fun Consultant','SAP JAVA Developer','SAP JVA Consultant','SAP LE Consultant','SAP LE Techno/Fun Consultant','SAP MDM Consultant','SAP MDM Techno/Fun Consultant','SAP MM Consultant','SAP MM Techno/Fun Consultant','SAP Netweaver Architect /Consultant','SAP PI Developer','SAP PLM Consultant','SAP PLM Techno/Fun Consultant','SAP PM CONSULTANT','SAP PM Techno/Fun Consultant','SAP PMO','SAP Portal Developer','SAP PP Consultant','SAP PP Techno/Fun Consultant','SAP PP-PI','SAP Project Manager','SAP PS Consultant','SAP PS Techno/Fun Consultant','SAP PS-CD Consultant','SAP PS-CD Techno/Functional Consultant','SAP QM Consultant','SAP QM Techno/Fun Consultant','SAP RE Consultant','SAP RE Techno/Fun Consultant','SAP Recruitment Consultant','SAP REFX','SAP RM Consultant','SAP RM Techno/Fun Consultant','SAP RM-CA','SAP SCM Business Analyst','Sap Sd Consultant','SAP SD Techno/Fun Consultant','SAP Security','SAP Security & Authorisations Consultant','SAP SNP Consultant','SAP SNP Techno/Functional Consultant','SAP Solution Architect','SAP Solution Manager Consultant','SAP SRM Consultant','SAP SRM Techno/Fun Consultant','SAP Tester','SAP TM Consultant','SAP TM Techno/Fun Consultant','Sap Trainer','SAP Treasury Consultant','SAP TRM Consultant','SAP TRM Techno/Functional Consultant','SAP UI5','SAP VARIANT CONFIGURATOR','SAP VC Consultant','SAP VC Techno/Fun Consultant','SAP VIM Consultant','SAP VIM Techno/Fun Consultant','SAP WM Consultant','SAP WM Techno/Fun Consultant','SAP Workflow Developer','SAS','SASS','Sauvegarde','Scala','SCM','Scrum Master','SD','SDH/PDH','SDLC','SEC Business Analayst Security','SEC Business Continuity','Sec Cert CCNA','SEC cert CEH','SEC cert CISA','SEC cert CISM','SEC cert CISP','SEC cert CISSP','SEC cert COBIT','SEC cert GSEC','SEC cert ISO 27001','SEC cert ITIL','SEC cert PCI DSS','SEC Checkpoint','SEC Cisco','SEC CISO','SEC Cloud Security','SEC Cryptography','SEC Cyber Architect','SEC Cyber Manager','SEC Cyber Security Governance','SEC Cyber Senior Manager','SEC Data Protection','SEC Digital Forensics','SEC Disaster Recovery','SEC Encryption','SEC FIREWALL','SEC GDPR','SEC Hacking Android','SEC IAM Business Analyst','SEC IAM Technical','SEC Infrastructure','SEC IOT-Security','SEC IT AUDIT','SEC IT Auditor','SEC IT Service Management','SEC Juniper','SEC KPI','SEC Log Mgt','SEC MALWARE','SEC Network Security','SEC PEN TEST','SEC PKI','SEC Pre-Sales Security','SEC Sales Security','SEC Scada Security','SEC Security Risk and Governance','SEC Security Strategy','SEC SIEM Threat Mgt','SEC SIEM Vulnerability Asst','SEC SIEM Vulnerability Mgt','SEC SOC Security Analyst','SEC TCP/IP','SEC Virtualization','SEC Web Application Security','SEC Wireless Security','Security','Security Cleared - UK Current','Security Cleared - UK Potential','Security Engineer','Seeburger','Selenium','Self-Service','SELLIGENT','Selligent Functional','Selligent Technical','sencha','SENTINEL','SEO Consultant','Serbian','Service Grid','Service Manager','Servicenow','Sharepoint','SHELL','Siebel','Siebel Adapter','Siebel Administrator','Siebel AMOA','Siebel AMOE','Siebel Architect','Siebel Business Analyst','Siebel Developer','Siebel Project Manager','SIEBEL TESTER','Siebel Trainer','SIEBELTOOLS','Silex','SINATRA Framework','Sitecore','SLA','SLOVAKIAN','Slovenian','SMTP','SNMP','SNP','SOA','SOA / BPM','SOA Architect','SOAP','SOD','Solaris','Solution Architect','Solution Design','Spanish','Spark','SPIP','Splunk','Spotfire','Spring','SQL','SQL Server','Sql Server Administrator','Sqoop','SQR','SRM','SS','SSAS','SSIS','SSO','SSRS','Stakeholder Management','Startup','STLC','Stockage','STORM','StreamServe','Successfactor Consultant','SuccessFactors Compensation','Successfactors Employee Central','Successfactors Functional Consultant','Successfactors LMS','SuccessFactors Onboarding','Successfactors PM/GM','SuccessFactors Recruitment','Successfactors Recruting','Successfactors Technical Consultant','SuccessFactors Variable Pay','SuccessFactors Workforce Analytics','Sud Est','Sud Ouest','Sun','Supply Chain','Supply Chain AX Module','Support','SUSE','Swedish','Swift','SWING','Sybase','Symfony','System Administrator','System Administrator Linux','System Administrator Unix','System Administrator Windows','System Engineer','System Engineer IBM','System Engineer Linux','System Engineer Unix','System Engineer Windows','T&E','T&L','Tableau','Talend','Talentsoft Consultant','Taleo','TAM','TCP/IP','TDD','Technical analyst','Technical Architect','Technical AX','Technical Lead','Technical NAV','Technical Project Manager','TELCO','Telco Design','Telco Installation Integration','Telco Network','Telco Project Manager','Telco Support','Telco Testing','Telecom Operational Manager','Telecom Project Manager','Telecom Transitional Manager','Telecoms Engineer','Telesales','Teradata','Test Analyst','Test Engineer','Test Manager','Tester','Thai','Tibco','TIBCO ARCHITECT','Tibco Business Analyst','TIBCO Developer','Tibco Project Manager','Tibco Systems Administrator','Tibco Tester','Titanium','Tivoli','Toad','TOGAF','TR','Trade and Logistics AX Module','Trainer','Trainer AX','Transition Manager','Transmission','Turkish','Tuxedo','TV','Typo3','UCM','UI designer','UI/User Interface','Ukrainian','UML','Umts','UNIX','Update Software Functional','Update Software Technical','UX design','UX/User Experience','VBS','VC','Vietnamese','VirtualBox','Virtualization','VLAN','VMWARE','VNC','VoIP','Vsphere','VueJs 2','WAN','Waterfall','Web ADI','Web architect','Web Design','Web Development','webdynpro','Webfocus','WebLogic','WebLogic Architect','Weblogic Developer','WebMethods','Webmethods architect','Webmethods Developer','Webmethods Project Manager','WebMethods Systems Administrator','webservices','Websphere','Websphere Architect','Websphere business Integrator','WebSphere Business Integrator Message Broker','Websphere Developer','WebSphere Project Manager','WebSphere Systems Administrator','WebSphere Tester','Wi-Fi','WiMax','Windows','Windows NT','Windows Phone','Windows Server','Winshuttle','Wip','Wireless','WM','WMQI','Wordpress','Workday Advanced Compensation','Workday Benefits','Workday Bonus','Workday Compensation','Workday Consultant','Workday Core Connector','Workday Data Conversion','Workday Engagement Manager','Workday Expenses','Workday Finance Functional Consultant','Workday Finance Technical Consultant','Workday French Payroll','Workday HCM Core Certified','Workday HCM Functional Consultant','Workday HCM Technical Consultant','Workday Integration Core','Workday Integrations Certified','Workday Learning','Workday Payroll','Workday Payroll Functional Consultant','Workday Payroll Technical Consultant','Workday Planning','Workday Pre-sales','Workday Project Manager','Workday Recruiting Certified','Workday Reporting','Workday Reporting Composite','Workday Security','Workday Studio','Workday Talent & Performance','Workday Testing','Workday Time and Absence','Workday Time Tracking','Workday Trainer','WorkFlow','WORKFORCE ADMINISTRATION','WPF','WPM','WSO2','WSS','Xamarin','Xcelsius','XHTML','XML','XML Publisher','XSLT','Yarn','Zend','Zookeeper')
--select * from SkillName
--select count(*) from SkillName --353157origin - 354.464/876.165 
select distinct SkillName, count(*)  as AMOUNT from SkillName  where SkillName is not null group by SkillName Order By SkillName --where BH_SkillList.name like 'SAP PP Consultant%';

, sfe as (
                select
                 C.candidateID, concat(C.FirstName,' ',C.LastName) as fullname
                 , 2983 as 'fe'
                --, SN.SkillName
                , case sn.SkillName
when '$Univers' then 856
when '.NET' then 857
when '1C' then 184
when '4G' then 858
when 'AA' then 859
when 'ABAP' then 860
when 'Abroad' then 861
when 'Absence' then 185
when 'Accountant' then 862
when 'Acd' then 863
when 'ActionScript' then 864
when 'Active Directory' then 865
when 'Active Enterprise' then 866
when 'Active Matrix' then 867
when 'ActiveX' then 186
when 'Actuary' then 868
when 'Actuate' then 187
when 'Adapter SDK' then 869
when 'ADI' then 870
when 'Administer Training' then 188
when 'Administer Workforce' then 189
when 'Administrator' then 871
when 'Adobe' then 872
when 'Adonix' then 873
when 'Adoption' then 874
when 'ADP' then 190
when 'ADSL' then 191
when 'Advanced Collections' then 875
when 'Advanced Configurator' then 192
when 'Advanced Planning' then 193
when 'AFF' then 876
when 'Afrikaans' then 194
when 'AFS' then 877
when 'Agile' then 195
when 'AIX' then 196
when 'Ajax' then 878
when 'ALE' then 197
when 'Alfresco' then 879
when 'AM' then 880
when 'Amazon Web Services' then 881
when 'AME' then 198
when 'AMF' then 882
when 'AMOA' then 883
when 'AMX BPM' then 199
when 'Analytics' then 884
when 'Android' then 200
when 'angular js' then 201
when 'AP' then 885
when 'Apache' then 202
when 'API' then 886
when 'APO' then 203
when 'Application designer' then 887
when 'Application developer' then 204
when 'Application engine' then 205
when 'Application Messaging' then 888
when 'Application Packages' then 889
when 'Application server' then 206
when 'Approval Management' then 890
when 'APS' then 891
when 'Aptitude' then 892
when 'AQ' then 207
when 'AquaLogic Architect' then 208
when 'AquaLogic BPM' then 209
when 'AquaLogic Developer' then 1490
when 'AquaLogic ESB' then 210
when 'Aqualogic' then 893
when 'AR' then 894
when 'Arabic' then 895
when 'Arbor' then 211
when 'Architect' then 212
when 'ARIBA' then 896
when 'Art Director' then 897
when 'AS/400' then 213
when 'AS3' then 898
when 'ASCP' then 214
when 'ASP.NET' then 899
when 'ATM' then 900
when 'Attendance Management' then 1491
when 'AX' then 901
when 'AX2009' then 902
when 'AX2012' then 903
when 'AX4' then 215
when 'Axapta' then 904
when 'Axure' then 216
when 'Azerbaijani' then 1492
when 'Azure AAD' then 905
when 'B2B Sales/Recruitment Consultant' then 217
when 'Back-End Developer' then 218
when 'BACKBONE' then 906
when 'Bale' then 907
when 'BAM' then 219
when 'Bank Reconciliation' then 908
when 'Banking Consultant' then 909
when 'BAPI' then 220
when 'Bash' then 910
when 'Basis' then 911
when 'BC' then 221
when 'BCS' then 912
when 'BEM' then 1493
when 'Benefits' then 222
when 'BI Architect' then 224
when 'bi publisher' then 225
when 'BI' then 223
when 'BI-IP' then 229
when 'BIA' then 913
when 'Big Data Analyst' then 914
when 'Big Data Architect' then 227
when 'Big Data Chief Officer' then 915
when 'Big Data Developer' then 916
when 'BIG DATA Ecole Gr.1' then 917
when 'Big Data Engineer' then 918
when 'Big Data Integration Specialist' then 919
when 'Big Data Miner' then 920
when 'BIG DATA Scientist' then 228
when 'Big Data Visualizer' then 921
when 'Big Data' then 226
when 'Billing' then 230
when 'BizTalk 2009' then 231
when 'Biztalk 2010' then 232
when 'BizTalk Architect' then 922
when 'BizTalk Business Analyst' then 923
when 'Biztalk Developer' then 924
when 'BizTalk Functional Consultant' then 925
when 'BizTalk Systems Administrator' then 233
when 'BO' then 234
when 'BODI' then 235
when 'BOM' then 236
when 'Bootstrap' then 926
when 'BPM' then 237
when 'BPR' then 927
when 'BPS' then 928
when 'Brio' then 929
when 'BRM' then 930
when 'Bscs' then 238
when 'BSS Consultant' then 239
when 'BSS Solution Architect' then 240
when 'Bulgarian' then 241
when 'Business Analyst' then 242
when 'Business Intelligence Architect' then 244
when 'Business Intelligence Business Analyst' then 245
when 'Business Intelligence Developer' then 931
when 'Business Intelligence Functional Analyst' then 932
when 'Business Intelligence Project Director' then 933
when 'Business Intelligence Project Manager' then 246
when 'Business Intelligence Techno Functional Consultant' then 934
when 'Business Intelligence Trainer' then 935
when 'Business Intelligence' then 243
when 'Business Process Consultant' then 247
when 'Business Rules' then 248
when 'Business Transformation' then 936
when 'BusinessConnect' then 937
when 'BusinessEvents 2nd' then 1494
when 'BusinessEvents' then 249
when 'BusinessFactor' then 1495
when 'BusinessStudio' then 938
when 'BusinessWorks 2nd' then 250
when 'BusinessWorks 6' then 940
when 'BusinessWorks' then 939
when 'BW' then 251
when 'C#' then 253
when 'C' then 252
when 'C++' then 941
when 'Ca' then 254
when 'Campus Solutions' then 942
when 'Candidate Gateway' then 255
when 'Cantonese' then 256
when 'Capital Market Consultant' then 943
when 'Captiva' then 257
when 'Cassandra' then 258
when 'CC' then 944
when 'CCNA/CCNP' then 945
when 'CDMA/WCDMA' then 259
when 'Centre' then 946
when 'Change management' then 947
when 'Charging Systems' then 948
when 'Checkpoint' then 949
when 'Chinese' then 950
when 'Chronos' then 951
when 'CI' then 952
when 'Cim' then 260
when 'Cisco' then 261
when 'CITRIX' then 953
when 'Clementine' then 954
when 'Clojure' then 262
when 'Cloud Architect' then 264
when 'Cloud Development' then 265
when 'Cloud Security' then 266
when 'cloud' then 263
when 'cloudera' then 267
when 'CM' then 268
when 'CMS' then 269
when 'CO' then 955
when 'Coach Agile' then 270
when 'COBOL' then 956
when 'CODA' then 271
when 'Cognos TM1' then 272
when 'Cognos' then 957
when 'Cold Fusion' then 958
when 'Collaborator' then 273
when 'Compensation' then 959
when 'Compliance Consultant' then 960
when 'COMPONENT INTERFACE' then 961
when 'Configuration Manager' then 274
when 'Configurator' then 275
when 'Consultancy CRM' then 962
when 'Consulting' then 276
when 'CONTROL M' then 277
when 'Copywriting' then 963
when 'CORBA' then 964
when 'Cordova' then 965
when 'Core Networks' then 278
when 'Cornerstone' then 279
when 'CouchDB' then 280
when 'CRM' then 281
when 'Croatian' then 282
when 'CROSS-PLATFORM' then 966
when 'CrossWorlds' then 283
when 'Crystal' then 967
when 'CS' then 968
when 'CSS' then 969
when 'CTO' then 284
when 'Customer Service CRM Module' then 970
when 'Customer Success Manager' then 971
when 'Cyber Security' then 972
when 'Czech' then 285
when 'd3js' then 286
when 'DADS-U' then 287
when 'Danish' then 288
when 'Data Analysis' then 289
when 'Data Architecture' then 973
when 'Data Centre' then 974
when 'Data Cleansing' then 290
when 'DATA INTEGRATION' then 291
when 'Data Migration' then 292
when 'Data mining' then 975
when 'Data Modelling' then 976
when 'Data Quality' then 293
when 'data science' then 294
when 'Data Warehousing' then 295
when 'Database Administrator' then 977
when 'Database Architect' then 296
when 'Database Design' then 978
when 'Database Development' then 979
when 'DATAMART' then 297
when 'Datamining' then 980
when 'DataStage' then 298
when 'DB2' then 981
when 'Debian' then 982
when 'Dell Quest One' then 983
when 'Dell Quest' then 299
when 'Delphi' then 984
when 'Demandware' then 985
when 'Developer' then 986
when 'DevOps' then 300
when 'DHCP' then 987
when 'Digital Payment Consultant' then 988
when 'Digital PM' then 301
when 'Director of Marketing and Communications' then 302
when 'Discoverer' then 989
when 'django' then 303
when 'DM' then 990
when 'DNS' then 991
when 'Documentum' then 304
when 'DOS' then 305
when 'DP' then 992
when 'Drupal' then 306
when 'DS' then 307
when 'Dutch' then 993
when 'Dynamics Functional' then 308
when 'Dynamics Technical' then 995
when 'Dynamics' then 994
when 'Dynasight' then 996
when 'E-commerce Development' then 1001
when 'e-commerce' then 1000
when 'EAI' then 997
when 'EAM' then 309
when 'EBTAX' then 310
when 'ECM' then 998
when 'Ecommerce' then 999
when 'eCommunication' then 1002
when 'eCompensation' then 1003
when 'ECONFIGURATOR' then 311
when 'eDevelopment' then 312
when 'EDI' then 1004
when 'eEnergy' then 1005
when 'EH&S' then 1006
when 'EIM' then 313
when 'ElasticSearch' then 1007
when 'Electronic Money' then 314
when 'ELM' then 1008
when 'eMarketing' then 315
when 'EmberJs' then 316
when 'Ems' then 317
when 'Engineer' then 318
when 'English' then 319
when 'ePerformance' then 1009
when 'epharma' then 1010
when 'EPM' then 1011
when 'eProcurement' then 1012
when 'eProfile' then 1013
when 'eRecruitment' then 320
when 'Erlang' then 1014
when 'ESA' then 321
when 'eSales' then 322
when 'eSettlement' then 323
when 'eSourcing' then 1015
when 'ESSBase' then 324
when 'Est' then 325
when 'eSupplier' then 1496
when 'ESX' then 1016
when 'Ethernet' then 326
when 'ETL' then 327
when 'Etraining' then 1017
when 'Ets' then 1018
when 'EX' then 328
when 'Exchange' then 1019
when 'Ext JS' then 1020
when 'EZpublish' then 329
when 'F&R' then 1021
when 'FA' then 330
when 'FAH' then 331
when 'FI' then 1022
when 'Fi-co' then 332
when 'Filenet' then 333
when 'Finance AX Module' then 334
when 'Finance Business Transformation' then 1023
when 'Finance NAV Module' then 335
when 'Financials' then 1024
when 'Finnish' then 1025
when 'Fintech' then 1026
when 'fiori' then 1027
when 'Firewall' then 336
when 'Flash' then 337
when 'Flume' then 338
when 'FM' then 339
when 'Foodtech' then 340
when 'Forms' then 341
when 'French' then 1028
when 'Front-End Developer' then 1029
when 'FS-CD' then 342
when 'FS-CM' then 344
when 'FS-CS' then 1030
when 'FS-PM' then 345
when 'FSAH' then 1497
when 'FSCM' then 343
when 'Full Stack' then 346
when 'Functional Architect' then 1032
when 'Functional AX' then 347
when 'Functional CRM' then 1033
when 'Functional NAV' then 348
when 'Functional' then 1031
when 'Fusion HR Consultant' then 349
when 'Fusion' then 1034
when 'GC' then 350
when 'Generix' then 1498
when 'Genesys' then 351
when 'Genio' then 1035
when 'German' then 1036
when 'GI' then 352
when 'GIT' then 353
when 'Gl' then 1037
when 'Global Payroll' then 354
when 'Golang' then 355
when 'Google Apps' then 1038
when 'Google Cloud Platform' then 1039
when 'Governance' then 1040
when 'Gprs' then 356
when 'Grants' then 1499
when 'GRC' then 1041
when 'Greek' then 1042
when 'Groovy' then 1043
when 'GSM' then 357
when 'GTA' then 1044
when 'Hadoop' then 1045
when 'Hal' then 358
when 'HANA' then 1046
when 'Hbase' then 359
when 'Hebrew' then 1047
when 'Helpdesk' then 1048
when 'Hibernate' then 360
when 'Hitachi' then 1049
when 'Hive' then 361
when 'hortonworks' then 362
when 'Hotonworks' then 1050
when 'HP Cloud' then 363
when 'HP-UX' then 1051
when 'HPQC' then 364
when 'HR Access Functional' then 1054
when 'HR Access Technical' then 1055
when 'HR ACCESS' then 1053
when 'HR AX Module' then 1056
when 'HR Business Transformation' then 1057
when 'HR' then 1052
when 'HRIS Project Manager' then 1058
when 'HTML' then 365
when 'HUM' then 1059
when 'Hungarian' then 1060
when 'HYBRID' then 366
when 'Hybride Cloud' then 367
when 'Hybris' then 1061
when 'Hyperion' then 1062
when 'IAM' then 368
when 'IBM BPM' then 369
when 'IBM cloud computing' then 1063
when 'IBM Identity Manager' then 370
when 'IBM MDM' then 371
when 'IDAM' then 372
when 'Identity Access Management' then 1064
when 'IDOC' then 1065
when 'IDS' then 1066
when 'IEXPENSES' then 1067
when 'IHM' then 373
when 'Ile de France' then 1068
when 'iLearning' then 374
when 'Indian' then 1069
when 'Infor M3' then 1070
when 'Informatica' then 1071
when 'Infrastructure' then 1072
when 'Integration Broker' then 1073
when 'Interfacing' then 375
when 'Interim Management' then 376
when 'INTERSHOP' then 1074
when 'INV' then 377
when 'ios developer' then 1075
when 'IOT' then 378
when 'IP' then 1076
when 'iProcess 2nd' then 1500
when 'iProcess' then 1077
when 'iProcurement' then 1078
when 'IRANIAN' then 379
when 'iRecruitment' then 1079
when 'IS Pharma' then 380
when 'IS Procurement' then 1501
when 'IS' then 1080
when 'IS-Auto' then 1081
when 'IS-FS' then 1082
when 'IS-H' then 384
when 'IS-Media' then 385
when 'IS-Mill' then 1083
when 'IS-OIL' then 1084
when 'IS-RE' then 386
when 'IS-Retail' then 1085
when 'IS-U' then 387
when 'ISAM' then 381
when 'ISAMESSO' then 382
when 'ISDI' then 1502
when 'ISDS' then 383
when 'ISIM' then 1503
when 'IT Recruitment Consultant' then 1504
when 'IT Security Recruitment Consultant' then 1086
when 'Italian' then 388
when 'ITIL' then 389
when 'J2EE' then 1087
when 'Jahia' then 1088
when 'Japanese' then 1089
when 'Java Architect' then 390
when 'Java Developer' then 391
when 'JAVA FX' then 392
when 'Java lead dev' then 393
when 'Java Project Manager' then 1091
when 'Java' then 1090
when 'Javascript' then 394
when 'JD EDWARDS' then 395
when 'JDEDWARDS' then 396
when 'Jira' then 397
when 'Joomla!' then 398
when 'jQuery' then 1092
when 'JSON' then 399
when 'JSP' then 400
when 'Juniper' then 1093
when 'Junit' then 1094
when 'Kafka' then 401
when 'Kanban' then 1095
when 'KAZAKH' then 402
when 'Kshell' then 403
when 'L4G' then 404
when 'LAN' then 1096
when 'Laravel' then 405
when 'Latvian' then 1097
when 'LE' then 406
when 'Lean' then 407
when 'Lease Management' then 408
when 'Linux' then 1098
when 'Lithuanian' then 409
when 'Logistics' then 1099
when 'Lombardi BPM' then 1505
when 'Lombardi Teamworks' then 1506
when 'Lotus Notes' then 1100
when 'Loyalty' then 410
when 'LSMW' then 1101
when 'LSO' then 411
when 'LTE' then 412
when 'MAC' then 1102
when 'Machine Learning' then 413
when 'Magento' then 1103
when 'Managing Director' then 1104
when 'Manufacturing AX Module' then 1106
when 'Manufacturing NAV Module' then 414
when 'Manufacturing' then 1105
when 'MapReduce' then 1107
when 'Marketing CRM Module' then 415
when 'Master Planning AX Module' then 1507
when 'Matlab' then 416
when 'Maven' then 417
when 'MDM' then 1108
when 'MDX' then 418
when 'MEAN Stack' then 419
when 'Mega Data' then 1109
when 'Mercury' then 420
when 'Message Broker' then 421
when 'Meteor JS' then 422
when 'Meteor' then 1110
when 'Microgen' then 1111
when 'Microsoft Azure' then 1112
when 'Microsoft BI' then 1113
when 'Microsoft CRM BA' then 1114
when 'Microsoft CRM Developer' then 424
when 'Microsoft CRM Director' then 425
when 'Microsoft CRM PM' then 1115
when 'Microsoft CRM' then 423
when 'Microsoft Dynamics AX Developer' then 1116
when 'Microsoft Dynamics AX Finance' then 427
when 'Microsoft Dynamics AX Functional Consultant' then 1117
when 'Microsoft Dynamics AX PM' then 428
when 'Microsoft Dynamics AX Pre-Sales' then 1118
when 'Microsoft Dynamics AX Sales' then 1119
when 'Microsoft Dynamics AX Solutions Architect' then 1120
when 'Microsoft Dynamics AX Support' then 1121
when 'Microsoft Dynamics AX Technical Consultant' then 429
when 'Microsoft Dynamics AX Test Analyst' then 1122
when 'Microsoft Dynamics AX' then 426
when 'Microsoft Dynamics CRM PM' then 431
when 'Microsoft Dynamics CRM' then 430
when 'Microsoft Dynamics NAV BA' then 433
when 'Microsoft Dynamics NAV Developer' then 434
when 'Microsoft Dynamics NAV Director' then 435
when 'Microsoft Dynamics NAV Functional' then 1123
when 'Microsoft Dynamics NAV PM' then 1124
when 'Microsoft Dynamics NAV pre-sales' then 436
when 'Microsoft Dynamics NAV Sales' then 1125
when 'Microsoft Dynamics NAV Support' then 1126
when 'Microsoft Dynamics NAV Trainer' then 1127
when 'microsoft dynamics nav' then 432
when 'MicroStrategy' then 1128
when 'Middle East' then 1129
when 'MM' then 1130
when 'Mobile Development' then 1131
when 'MongoDB' then 437
when 'Movex' then 1132
when 'MQ Series' then 1133
when 'Mqsi' then 1508
when 'MRO' then 1134
when 'MRP' then 438
when 'MSCA' then 439
when 'MUREX ARCHITECT' then 1135
when 'MySQL' then 440
when 'NAS' then 441
when 'Netbackup' then 442
when 'NETSUITE' then 1136
when 'Netweaver Gateway' then 443
when 'Netweaver' then 1137
when 'Network administrator' then 444
when 'Network engineer' then 445
when 'Networker' then 1138
when 'Noc' then 1139
when 'Node JS' then 1140
when 'NORD' then 446
when 'norweigan' then 447
when 'NoSQL' then 448
when 'Nvision' then 1141
when 'OAF' then 1142
when 'OAM' then 449
when 'OBIEE' then 1143
when 'Objective-C' then 1144
when 'Ocs' then 1145
when 'oData' then 1146
when 'ODI' then 450
when 'ODS' then 451
when 'OFA' then 1147
when 'OGSD' then 1509
when 'OIC' then 1148
when 'OIL HPM' then 452
when 'OIL SSR' then 1149
when 'OIL TD' then 1150
when 'OIL TSW' then 453
when 'OIM' then 1151
when 'OKC' then 1510
when 'OKE' then 1152
when 'OKF' then 1511
when 'OKS' then 454
when 'OLAP' then 1153
when 'OLM' then 455
when 'OM' then 456
when 'ONDEMAND' then 457
when 'OO Programming' then 1154
when 'OPEN' then 1155
when 'OpenStack' then 1156
when 'OPENTEXT' then 458
when 'OPM' then 459
when 'optimization' then 1157
when 'Oracle Administrator' then 461
when 'Oracle Database Developer' then 462
when 'Oracle EBS Administrator' then 1158
when 'Oracle EBS CRM Business Analyst' then 1159
when 'Oracle EBS CRM Functional Consultant' then 464
when 'Oracle EBS CRM Project Director' then 465
when 'Oracle EBS CRM Project Manager' then 466
when 'Oracle EBS CRM Support Consultant' then 1160
when 'Oracle EBS CRM Technical Analyst' then 1161
when 'Oracle EBS Financials Business Analyst' then 1162
when 'Oracle EBS Financials Functional Consultant' then 467
when 'Oracle EBS Financials Project Director' then 468
when 'Oracle EBS Financials Project Manager' then 1163
when 'Oracle EBS Financials Support Consultant' then 469
when 'Oracle EBS Financials Technical Analyst' then 1164
when 'Oracle EBS Financials Techno Functional' then 1512
when 'Oracle EBS HRMS Business Analyst' then 470
when 'Oracle EBS HRMS Functional Consultant' then 1165
when 'Oracle EBS HRMS Project Director' then 1166
when 'Oracle EBS HRMS Project Manager' then 471
when 'Oracle EBS HRMS Support Consultant' then 472
when 'Oracle EBS HRMS Technical Analyst' then 473
when 'Oracle EBS HRMS Techno Functional' then 474
when 'Oracle EBS Logistics Business Analyst' then 475
when 'Oracle EBS Logistics Functional Consultant' then 476
when 'Oracle EBS Logistics Project Director' then 477
when 'Oracle EBS Logistics Project Manager' then 1167
when 'Oracle EBS Logistics Support Consultant' then 1168
when 'Oracle EBS Logistics Technical Analyst' then 1513
when 'Oracle EBS Logistics Techno Functional' then 478
when 'Oracle EBS Trainer' then 1169
when 'Oracle EBS' then 463
when 'Oracle Fusion Financials' then 479
when 'Oracle Fusion HCM Functional Consultant' then 1171
when 'Oracle Fusion HCM Technical Consultant' then 1172
when 'Oracle Fusion Middleware' then 480
when 'Oracle Fusion PPM' then 1173
when 'Oracle Fusion Procurement' then 481
when 'Oracle Fusion' then 1170
when 'Oracle Identity Management' then 482
when 'Oracle SOA Architect' then 484
when 'Oracle SOA Developer' then 1174
when 'Oracle SOA Project Manager' then 1175
when 'Oracle SOA Systems Administrator' then 1176
when 'Oracle SOA Tester' then 485
when 'Oracle SOA' then 483
when 'Oracle' then 460
when 'Order Capture' then 1177
when 'Order Management' then 1178
when 'OSS Consultant' then 1179
when 'OSS Solution Architect' then 1180
when 'OST' then 486
when 'OTA' then 487
when 'OTC' then 488
when 'Other' then 1181
when 'OTL' then 489
when 'OTM' then 490
when 'Ouest' then 491
when 'PA' then 492
when 'PAB' then 493
when 'PAC' then 494
when 'Pascal' then 495
when 'Payment Consultant' then 1182
when 'Payment Processing' then 1183
when 'Payroll' then 1184
when 'PC' then 496
when 'Pega Architect' then 1185
when 'Pega CBA' then 1186
when 'Pega CCA' then 1187
when 'Pega CLSA' then 497
when 'Pega CSA' then 498
when 'Pega CSSA' then 1188
when 'PEGA Developer' then 499
when 'PEGA LSA' then 1189
when 'PEGA Project Manager' then 1514
when 'Pega PRPC' then 1190
when 'Pentaho' then 500
when 'Penthao' then 1191
when 'Peoplecode' then 1192
when 'Peoplesoft Administrator' then 1194
when 'Peoplesoft Architect' then 501
when 'PeopleSoft Campus Developer' then 502
when 'PeopleSoft Campus Functional' then 503
when 'PeopleSoft Campus Project Manager' then 504
when 'PeopleSoft Campus Techno Functional' then 1195
when 'PeopleSoft CRM Developer' then 1196
when 'PeopleSoft CRM Functional' then 505
when 'PeopleSoft CRM Project Manager' then 506
when 'PeopleSoft CRM Techno Functional' then 507
when 'PeopleSoft EPM Developer' then 1197
when 'PeopleSoft EPM Functional' then 1198
when 'PeopleSoft EPM Project Manager' then 508
when 'PeopleSoft EPM Techno Functional' then 509
when 'PeopleSoft ESA Developer' then 1199
when 'PeopleSoft ESA Functional' then 510
when 'PeopleSoft ESA Project Manager' then 1200
when 'PeopleSoft ESA Techno Functional' then 511
when 'PeopleSoft Financials Developer' then 512
when 'PeopleSoft Financials Functional' then 513
when 'PeopleSoft Financials Project Manager' then 1201
when 'PeopleSoft Financials Techno Functional' then 514
when 'PeopleSoft FSCM Developer' then 515
when 'PeopleSoft FSCM Functional' then 516
when 'PeopleSoft FSCM Project Manager' then 517
when 'PeopleSoft FSCM Techno Functional' then 1202
when 'PeopleSoft HR Developer' then 518
when 'PeopleSoft HR Functional' then 1203
when 'PeopleSoft HR Project Manager' then 519
when 'PeopleSoft HR Techno Functional' then 520
when 'PeopleSoft Payroll Developer' then 1204
when 'PeopleSoft Payroll Functional' then 521
when 'PeopleSoft Payroll Project Manager' then 522
when 'PeopleSoft Payroll Techno Functional' then 523
when 'PeopleSoft Project Director' then 1205
when 'Peoplesoft Tester' then 524
when 'PEOPLESOFT TRAINER' then 1206
when 'PeopleSoft' then 1193
when 'PeopleTools' then 525
when 'PERL' then 1207
when 'Persian' then 1208
when 'phonegap' then 1209
when 'PHP lead dev' then 527
when 'PHP' then 526
when 'PI' then 528
when 'PIG' then 1210
when 'PL/SQL' then 1211
when 'Play Framework' then 529
when 'PLM' then 1212
when 'PM' then 1213
when 'PM/Program Manager AX' then 1214
when 'PM/Program Manager CRM' then 1215
when 'PM/Program Manager NAV' then 530
when 'PMO' then 1216
when 'Po' then 1217
when 'Policy Manager' then 531
when 'Polish' then 532
when 'Portal' then 533
when 'Portuguese' then 534
when 'POS DM' then 1218
when 'Post Merger Integration Consultant' then 1219
when 'Post Production Engineer' then 1220
when 'Postgresql' then 535
when 'postsales' then 536
when 'PowerShell' then 537
when 'PP' then 1221
when 'Pre-Production Engineer' then 538
when 'Predictive Analytics' then 1222
when 'Presales' then 539
when 'Prestashop' then 540
when 'Pretashop' then 541
when 'Pricing' then 542
when 'Private Cloud' then 1223
when 'ProcessServer' then 543
when 'Procurement AX Module' then 544
when 'Procurement' then 1224
when 'PRODUCT CATALOGUE' then 545
when 'Product Director' then 546
when 'Product Manager' then 547
when 'Product Owner' then 1225
when 'Production Analyst' then 548
when 'Programme Director' then 1226
when 'Project Manager' then 549
when 'Prolog' then 1227
when 'PS' then 1228
when 'PS-Query' then 1229
when 'Public Cloud' then 1230
when 'Purchasing AX Module' then 550
when 'Python' then 551
when 'QlikView' then 1231
when 'QM' then 552
when 'QP' then 553
when 'QTP' then 1232
when 'Quality Engineer' then 1233
when 'Quality Management' then 554
when 'QualityCenter' then 555
when 'quant' then 556
when 'R&D' then 557
when 'R' then 1234
when 'R.' then 1235
when 'RAN' then 558
when 'RDJ' then 1236
when 'React JS' then 1237
when 'ReactJS' then 1238
when 'Recruitment Resourcer' then 560
when 'Recruitment' then 559
when 'Redhat' then 561
when 'Remedy' then 1239
when 'Rendezvous' then 1240
when 'Reports' then 1241
when 'Requirement Engineering' then 562
when 'Responsive Design' then 563
when 'Responsive Development' then 1242
when 'RESTFUL' then 1243
when 'Retail AX Module' then 564
when 'Retail' then 1244
when 'Risk Backtesting' then 565
when 'Risk Compliance Consultant' then 1245
when 'Risk Credit' then 566
when 'Risk EAD' then 1246
when 'Risk IFRS9' then 1247
when 'Risk LGD' then 567
when 'Risk Management' then 568
when 'Risk market frtb' then 1248
when 'Risk market quant' then 569
when 'Risk market regulatory' then 570
when 'Risk MOA' then 571
when 'Risk PD' then 572
when 'Risk Score Carding' then 573
when 'Risk Stress Testing' then 574
when 'RM' then 1249
when 'RMCA' then 575
when 'Romanian' then 576
when 'RPG' then 577
when 'Ruby on Rails' then 1250
when 'Ruby' then 578
when 'Russian' then 1251
when 'SA' then 579
when 'SAAS' then 1252
when 'Sage 100' then 1253
when 'Sage 1000' then 581
when 'Sage CRM' then 1254
when 'Sage Paie' then 582
when 'Sage x3' then 583
when 'SAGE' then 580
when 'SAH' then 1515
when 'Sales and Marketing NAV Module' then 584
when 'Sales AX Module' then 585
when 'Sales CRM Module' then 1256
when 'Sales Director' then 1257
when 'Sales' then 1255
when 'Salesforce' then 1258
when 'Salesforce.com Administrator' then 586
when 'Salesforce.com Architect' then 1259
when 'Salesforce.com Functional' then 587
when 'Salesforce.com Project Manager' then 588
when 'Salesforce.com Technical' then 1260
when 'SAP ABAP Developer' then 590
when 'SAP Adapter' then 1261
when 'SAP Admin' then 591
when 'SAP AMOA Finance Consultant' then 592
when 'SAP AMOA Logistic Consultant' then 593
when 'SAP APO CONSULTANT' then 594
when 'SAP APO Techno/Fun Consultant' then 1262
when 'SAP Bank Analyzer Consultant' then 595
when 'SAP Bank Analyzer Techno/Fun Consultant' then 596
when 'SAP BASIS Administrator/Consultant' then 1263
when 'SAP BCA Consultant' then 597
when 'SAP BCA Techno/Fun Consultant' then 1264
when 'SAP BCS Consultant' then 598
when 'SAP BCS Techno/Fun Consultant' then 599
when 'SAP BFC Consultant' then 600
when 'SAP BI Consultant' then 601
when 'SAP BI Techno-Functional Consultant' then 1265
when 'SAP BI-IP Consultant' then 1266
when 'SAP BI-IP Techno/Fun Consultant' then 1267
when 'SAP BO Administrator' then 602
when 'SAP BO Consultant' then 1268
when 'SAP BO Techno/Fun Consultant' then 1269
when 'SAP BPC Consultant' then 603
when 'SAP BRF' then 604
when 'SAP BRIM Consultant' then 605
when 'Sap Business Analyst' then 1270
when 'SAP BW Consultant' then 606
when 'Sap Bw Developer' then 1271
when 'SAP BW Techno/Fun Consultant' then 607
when 'SAP CC Consultant' then 1516
when 'SAP CFM Consultant' then 608
when 'SAP CFM Techno/Fun Consultant' then 609
when 'SAP CML Consultant' then 610
when 'SAP CML Techno/Fun Consultant' then 611
when 'SAP CO Consultant' then 612
when 'SAP CO Techno/Fun Consultant' then 613
when 'SAP Core Banking Consultant' then 614
when 'SAP Core Banking Techno/Fun Consultant' then 615
when 'Sap Crm Consultant' then 1272
when 'SAP CRM Developer' then 616
when 'SAP CS Consultant' then 617
when 'SAP CS Techno/Fun Consultant' then 618
when 'SAP CTRM Consultant' then 1273
when 'SAP CTRM Techno/Functional Consultant' then 1274
when 'SAP DATA MIGRATION CONSULTANT' then 619
when 'SAP DM Consultant' then 1275
when 'SAP DM Techno/Fun Consultant' then 1276
when 'SAP EDI Techno/Fun Consultant' then 620
when 'SAP EHS Consultant' then 1277
when 'SAP EM Consultant' then 621
when 'SAP EM Techno/Fun Consultant' then 1517
when 'SAP EWM Consultant' then 622
when 'SAP EWM Techno/Fun Consultant' then 1278
when 'Sap Fi Consultant' then 623
when 'SAP FI Techno/Fun Consultant' then 624
when 'SAP FI-AA Consultant' then 1279
when 'SAP FI-AA Techno/Functional Consultant' then 625
when 'SAP FI-CA Consultant' then 1280
when 'SAP Financials Business Analyst' then 626
when 'SAP Fiori' then 1281
when 'SAP FM Consultant' then 1282
when 'SAP FM Techno/Fun Consultant' then 627
when 'SAP FS-BP Consultant' then 1283
when 'SAP FS-CD Consultant' then 628
when 'SAP FS-CM Consultant' then 1284
when 'SAP FS-CS Consultant' then 629
when 'SAP FS-ICM Consultant' then 1285
when 'SAP FS-PM Consultant' then 630
when 'SAP FS-RI Consultant' then 631
when 'SAP GRC Consultant' then 1286
when 'SAP GRC Techno/Fun Consultant' then 1287
when 'SAP GTS Consultant' then 1288
when 'SAP GTS Techno/Fun Consultant' then 1289
when 'SAP HANA Consultant' then 1290
when 'SAP HCM Business Analyst' then 1291
when 'SAP HCM Consultant' then 1292
when 'SAP HCM Developer' then 1293
when 'SAP HCM Techno/Fun Consultant' then 632
when 'SAP HR' then 633
when 'SAP HUM Consultant' then 1294
when 'SAP IPM' then 1295
when 'SAP IS-A&D Consultant' then 634
when 'SAP IS-A&D Techno/Fun Consultant' then 1518
when 'SAP IS-AFS Consultant' then 635
when 'SAP IS-AFS Techno/Fun Consultant' then 636
when 'SAP IS-Auto Consultant' then 637
when 'SAP IS-Auto Techno/Fun Consultant' then 638
when 'SAP IS-Bev Consultant' then 1296
when 'SAP IS-EH&S Consultant' then 1297
when 'SAP IS-EH&S Techno/Fun Consultant' then 639
when 'SAP IS-FS Consultant' then 1519
when 'SAP IS-FS Techno/Fun Consultant' then 1520
when 'SAP IS-H Consultant' then 1298
when 'SAP IS-H Techno/Fun Consultant' then 1299
when 'SAP IS-Media Consultant' then 1300
when 'SAP IS-Media Techno/Fun Consultant' then 640
when 'SAP IS-Mill Consultant' then 641
when 'SAP IS-Mill Techno/Fun Consultant' then 1521
when 'SAP IS-OIL CONSULTANT' then 1301
when 'SAP IS-Oil Techno/Fun Consultant' then 1302
when 'SAP IS-Procurement Techno/Functional Consultant' then 1303
when 'SAP IS-PS Consultant' then 1304
when 'SAP IS-RE Consultant' then 642
when 'SAP IS-RE Techno/Fun Consultant' then 1522
when 'SAP IS-Retail Consultant' then 643
when 'SAP IS-Retail Techno/Fun Consultant' then 644
when 'SAP IS-T Consultant' then 1305
when 'SAP IS-T Techno/Fun Consultant' then 645
when 'SAP IS-U CONSULTANT' then 646
when 'SAP IS-U Techno/Fun Consultant' then 1306
when 'SAP IS-Waste Consultant' then 1489
when 'SAP JAVA Developer' then 1307
when 'SAP JVA Consultant' then 647
when 'SAP LE Consultant' then 648
when 'SAP LE Techno/Fun Consultant' then 649
when 'SAP MDM Consultant' then 1308
when 'SAP MDM Techno/Fun Consultant' then 1309
when 'SAP MM Consultant' then 1310
when 'SAP MM Techno/Fun Consultant' then 650
when 'SAP Netweaver Architect /Consultant' then 651
when 'SAP PI Developer' then 1311
when 'SAP PLM Consultant' then 1312
when 'SAP PLM Techno/Fun Consultant' then 1313
when 'SAP PM CONSULTANT' then 1314
when 'SAP PM Techno/Fun Consultant' then 652
when 'SAP PMO' then 653
when 'SAP Portal Developer' then 1315
when 'SAP PP Consultant' then 1316
when 'SAP PP Techno/Fun Consultant' then 654
when 'SAP PP-PI' then 655
when 'SAP PRA Consultant' then 1523
when 'SAP Project Manager' then 1317
when 'SAP PS Consultant' then 656
when 'SAP PS Techno/Fun Consultant' then 1318
when 'SAP PS-CD Consultant' then 657
when 'SAP PS-CD Techno/Functional Consultant' then 658
when 'SAP QM Consultant' then 659
when 'SAP QM Techno/Fun Consultant' then 660
when 'SAP RE Consultant' then 661
when 'SAP RE Techno/Fun Consultant' then 1319
when 'SAP Recruitment Consultant' then 662
when 'SAP REFX' then 663
when 'SAP RM Consultant' then 664
when 'SAP RM Techno/Fun Consultant' then 1320
when 'SAP RM-CA' then 1321
when 'SAP SCM Business Analyst' then 665
when 'Sap Sd Consultant' then 1322
when 'SAP SD Techno/Fun Consultant' then 1323
when 'SAP Security & Authorisations Consultant' then 1325
when 'SAP Security' then 1324
when 'SAP SNP Consultant' then 666
when 'SAP SNP Techno/Functional Consultant' then 1326
when 'SAP Solution Architect' then 1327
when 'SAP Solution Manager Consultant' then 1328
when 'SAP SRM Consultant' then 667
when 'SAP SRM Techno/Fun Consultant' then 1329
when 'SAP Tester' then 1330
when 'SAP TM Consultant' then 668
when 'SAP TM Techno/Fun Consultant' then 1331
when 'Sap Trainer' then 669
when 'SAP Treasury Consultant' then 1332
when 'SAP TRM Consultant' then 1333
when 'SAP TRM Techno/Functional Consultant' then 1334
when 'SAP TV Consultant' then 1524
when 'SAP UI5' then 1335
when 'SAP VARIANT CONFIGURATOR' then 670
when 'SAP VC Consultant' then 1336
when 'SAP VC Techno/Fun Consultant' then 1337
when 'SAP VIM Consultant' then 671
when 'SAP VIM Techno/Fun Consultant' then 672
when 'SAP WM Consultant' then 1338
when 'SAP WM Techno/Fun Consultant' then 1339
when 'SAP Workflow Developer' then 673
when 'SAP' then 589
when 'SAS' then 674
when 'SASS' then 1340
when 'Sauvegarde' then 1341
when 'Scala' then 1342
when 'SCM' then 1343
when 'Scrum Master' then 1344
when 'SD' then 1345
when 'SDH/PDH' then 675
when 'SDLC' then 676
when 'SEC Business Analayst Security' then 1346
when 'SEC Business Continuity' then 677
when 'Sec Cert CCNA' then 678
when 'SEC cert CEH' then 1347
when 'SEC cert CISA' then 1348
when 'SEC cert CISM' then 679
when 'SEC cert CISP' then 680
when 'SEC cert CISSP' then 681
when 'SEC cert COBIT' then 1349
when 'SEC cert GSEC' then 1350
when 'SEC cert ISO 27001' then 682
when 'SEC cert ITIL' then 1351
when 'SEC cert PCI DSS' then 1352
when 'SEC Checkpoint' then 1353
when 'SEC Cisco' then 1354
when 'SEC CISO' then 683
when 'SEC Cloud Security' then 1355
when 'SEC Cryptography' then 684
when 'SEC Cyber Architect' then 685
when 'SEC Cyber Manager' then 686
when 'SEC Cyber Security Governance' then 1356
when 'SEC Cyber Senior Manager' then 1357
when 'SEC Data Protection' then 687
when 'SEC Digital Forensics' then 1358
when 'SEC Disaster Recovery' then 688
when 'SEC Encryption' then 689
when 'SEC FIREWALL' then 690
when 'SEC GDPR' then 691
when 'SEC Hacking Android' then 1359
when 'SEC IAM Business Analyst' then 1360
when 'SEC IAM Technical' then 692
when 'SEC Infrastructure' then 693
when 'SEC IOT-Security' then 694
when 'SEC IT AUDIT' then 695
when 'SEC IT Auditor' then 696
when 'SEC IT Service Management' then 1361
when 'SEC Juniper' then 697
when 'SEC KPI' then 1362
when 'SEC Log Mgt' then 1363
when 'SEC MALWARE' then 1364
when 'SEC Network Security' then 698
when 'SEC PEN TEST' then 1365
when 'SEC PKI' then 1366
when 'SEC Pre-Sales Security' then 699
when 'SEC Sales Security' then 700
when 'SEC Scada Security' then 1367
when 'SEC Security Risk and Governance' then 701
when 'SEC Security Strategy' then 1368
when 'SEC SIEM Threat Mgt' then 1369
when 'SEC SIEM Vulnerability Asst' then 702
when 'SEC SIEM Vulnerability Mgt' then 1370
when 'SEC SOC Security Analyst' then 703
when 'SEC TCP/IP' then 704
when 'SEC Virtualization' then 1371
when 'SEC Web Application Security' then 1372
when 'SEC Wireless Security' then 705
when 'Security Cleared - UK Current' then 707
when 'Security Cleared - UK Potential' then 708
when 'Security Engineer' then 709
when 'Security' then 706
when 'Seeburger' then 710
when 'Selenium' then 711
when 'Self-Service' then 712
when 'Selligent Functional' then 713
when 'Selligent Technical' then 1374
when 'SELLIGENT' then 1373
when 'sencha' then 1375
when 'SENTINEL' then 714
when 'SEO Consultant' then 715
when 'Serbian' then 1376
when 'Service Grid' then 1377
when 'Service Manager' then 1378
when 'Servicenow' then 716
when 'Sharepoint' then 717
when 'SHELL' then 1379
when 'Siebel Adapter' then 1381
when 'Siebel Administrator' then 1382
when 'Siebel AMOA' then 1383
when 'Siebel AMOE' then 718
when 'Siebel Architect' then 719
when 'Siebel Business Analyst' then 1384
when 'Siebel Developer' then 1385
when 'Siebel Project Manager' then 720
when 'SIEBEL TESTER' then 1386
when 'Siebel Trainer' then 721
when 'Siebel' then 1380
when 'SIEBELTOOLS' then 722
when 'Silex' then 723
when 'SINATRA Framework' then 1387
when 'Sitecore' then 724
when 'SLA' then 725
when 'SLOVAKIAN' then 726
when 'Slovenian' then 1388
when 'SMTP' then 727
when 'SNMP' then 1389
when 'SNP' then 1390
when 'SOA / BPM' then 729
when 'SOA Architect' then 730
when 'SOA' then 728
when 'SOAP' then 1391
when 'SOD' then 1392
when 'Solaris' then 731
when 'Solution Architect' then 732
when 'Solution Design' then 1393
when 'Spanish' then 1394
when 'Spark' then 1395
when 'SPIP' then 733
when 'Splunk' then 734
when 'Spotfire' then 1396
when 'Spring' then 1397
when 'Sql Server Administrator' then 736
when 'SQL Server' then 735
when 'SQL' then 1398
when 'Sqoop' then 737
when 'SQR' then 738
when 'SRM' then 1399
when 'SS' then 1400
when 'SSAS' then 1401
when 'SSIS' then 739
when 'SSO' then 740
when 'SSRS' then 741
when 'Stakeholder Management' then 1402
when 'Startup' then 742
when 'STLC' then 743
when 'Stockage' then 744
when 'STORM' then 1403
when 'StreamServe' then 1404
when 'Successfactor Consultant' then 745
when 'SuccessFactors Compensation' then 1405
when 'Successfactors Employee Central' then 1406
when 'Successfactors Functional Consultant' then 746
when 'Successfactors LMS' then 1407
when 'SuccessFactors Onboarding' then 1408
when 'Successfactors PM/GM' then 747
when 'SuccessFactors Recruitment' then 748
when 'Successfactors Recruting' then 749
when 'Successfactors Technical Consultant' then 1409
when 'SuccessFactors Variable Pay' then 750
when 'SuccessFactors Workforce Analytics' then 751
when 'Sud Est' then 752
when 'Sud Ouest' then 753
when 'Sun' then 754
when 'Supply Chain AX Module' then 756
when 'Supply Chain' then 755
when 'Support' then 757
when 'SUSE' then 1410
when 'Swedish' then 758
when 'Swift' then 1411
when 'SWING' then 759
when 'Sybase' then 760
when 'Symfony' then 761
when 'System Administrator Linux' then 762
when 'System Administrator Unix' then 763
when 'System Administrator Windows' then 1413
when 'System Administrator' then 1412
when 'System Engineer IBM' then 764
when 'System Engineer Linux' then 1415
when 'System Engineer Unix' then 765
when 'System Engineer Windows' then 766
when 'System Engineer' then 1414
when 'T&E' then 1416
when 'T&L' then 767
when 'Tableau' then 768
when 'Talend' then 1417
when 'Talentsoft Consultant' then 769
when 'Taleo' then 1418
when 'TAM' then 770
when 'TCP/IP' then 1419
when 'TDD' then 1420
when 'Technical analyst' then 1421
when 'Technical Architect' then 1422
when 'Technical AX' then 1423
when 'Technical Lead' then 1424
when 'Technical NAV' then 1425
when 'Technical Project Manager' then 771
when 'Telco Design' then 773
when 'Telco Installation Integration' then 774
when 'Telco Network' then 1426
when 'Telco O&M' then 1525
when 'Telco PMO' then 1526
when 'Telco Project Manager' then 775
when 'Telco Support' then 1427
when 'Telco Testing' then 776
when 'TELCO' then 772
when 'Telecom Operational Manager' then 1428
when 'Telecom Project Manager' then 777
when 'Telecom Transitional Manager' then 1429
when 'Telecoms Engineer' then 1430
when 'Telesales' then 1431
when 'Teradata' then 1432
when 'Test Analyst' then 778
when 'Test Engineer' then 779
when 'Test Manager' then 1433
when 'Tester' then 1434
when 'Thai' then 780
when 'TIBCO ARCHITECT' then 1436
when 'Tibco Business Analyst' then 1437
when 'TIBCO Developer' then 781
when 'Tibco MDM' then 1527
when 'Tibco Project Manager' then 782
when 'Tibco Systems Administrator' then 783
when 'Tibco Tester' then 1438
when 'Tibco' then 1435
when 'Titanium' then 1439
when 'Tivoli' then 784
when 'Toad' then 1440
when 'TOGAF' then 785
when 'TR' then 1441
when 'Trade and Logistics AX Module' then 1442
when 'Trainer AX' then 1443
when 'Trainer NAV' then 1528
when 'Trainer' then 786
when 'Transition Manager' then 787
when 'Transmission' then 788
when 'Turkish' then 789
when 'Tuxedo' then 1444
when 'TV' then 790
when 'Typo3' then 791
when 'UCM' then 792
when 'UI designer' then 793
when 'UI/User Interface' then 794
when 'Ukrainian' then 795
when 'UML' then 1445
when 'Umts' then 796
when 'UNIX' then 1446
when 'Update Software Functional' then 1447
when 'Update Software Technical' then 797
when 'UTRAN' then 1529
when 'UX design' then 1448
when 'UX/User Experience' then 1449
when 'VBS' then 798
when 'VC' then 1450
when 'Vietnamese' then 1451
when 'VirtualBox' then 799
when 'Virtualization' then 800
when 'VLAN' then 1452
when 'VMWARE' then 1453
when 'VNC' then 801
when 'VoIP' then 802
when 'Vsphere' then 803
when 'VueJs 2' then 1454
when 'WAN' then 804
when 'Waterfall' then 1455
when 'Web ADI' then 805
when 'Web architect' then 806
when 'Web Design' then 807
when 'Web Development' then 808
when 'webdynpro' then 809
when 'Webfocus' then 810
when 'WebLogic Architect' then 1457
when 'Weblogic Developer' then 1458
when 'WebLogic Integrator' then 1530
when 'WebLogic' then 1456
when 'Webmethods architect' then 812
when 'Webmethods Developer' then 1459
when 'Webmethods Project Manager' then 813
when 'WebMethods Systems Administrator' then 814
when 'WebMethods Tester' then 1531
when 'WebMethods' then 811
when 'webservices' then 1460
when 'Websphere Architect' then 1461
when 'WebSphere Business Integrator Message Broker' then 817
when 'Websphere business Integrator' then 816
when 'Websphere Developer' then 818
when 'WebSphere Project Manager' then 1462
when 'WebSphere Systems Administrator' then 819
when 'WebSphere Tester' then 820
when 'Websphere' then 815
when 'Wi-Fi' then 1463
when 'WiMax' then 1464
when 'Windows NT' then 821
when 'Windows Phone' then 1466
when 'Windows Server' then 822
when 'Windows' then 1465
when 'Winshuttle' then 1467
when 'Wip' then 1468
when 'Wireless' then 1469
when 'WM' then 823
when 'WMQI' then 824
when 'Wordpress' then 1470
when 'Workday Advanced Compensation' then 825
when 'Workday Benefits' then 1471
when 'Workday Bonus' then 826
when 'Workday Compensation' then 1472
when 'Workday Consultant' then 827
when 'Workday Core Connector' then 828
when 'Workday Data Conversion' then 829
when 'Workday Engagement Manager' then 830
when 'Workday Expenses' then 1473
when 'Workday Finance Functional Consultant' then 831
when 'Workday Finance Technical Consultant' then 1474
when 'Workday French Payroll' then 1475
when 'Workday HCM Core Certified' then 1476
when 'Workday HCM Functional Consultant' then 832
when 'Workday HCM Technical Consultant' then 833
when 'Workday Integration Core' then 834
when 'Workday Integrations Certified' then 835
when 'Workday Learning' then 836
when 'Workday Payroll Functional Consultant' then 1477
when 'Workday Payroll Technical Consultant' then 1478
when 'Workday Payroll' then 837
when 'Workday Planning' then 838
when 'Workday Pre-sales' then 839
when 'Workday Project Manager' then 1479
when 'Workday Recruiting Certified' then 1480
when 'Workday Reporting Composite' then 1481
when 'Workday Reporting' then 840
when 'Workday Security' then 841
when 'Workday Studio' then 842
when 'Workday Talent & Performance' then 843
when 'Workday Testing' then 1482
when 'Workday Time and Absence' then 1483
when 'Workday Time Tracking' then 844
when 'Workday Trainer' then 1484
when 'WorkFlow' then 1485
when 'WORKFORCE ADMINISTRATION' then 845
when 'WPA' then 1532
when 'WPF' then 846
when 'WPM' then 847
when 'WSO2' then 848
when 'WSS' then 1486
when 'Xamarin' then 849
when 'Xcelsius' then 850
when 'xECP' then 1533
when 'XHTML' then 851
when 'XML Publisher' then 853
when 'XML' then 852
when 'XSLT' then 854
when 'Yarn' then 1487
when 'Z/OS' then 1534
when 'Zend' then 1488
when 'Zookeeper' then 855
end as 'sfe'
                from bullhorn1.Candidate C
                left join SkillName SN on C.candidateID = SN.candidateID
                --where C.isPrimaryOwner = 1 and  SN.userId is not null
)
select count(*) from sfe where sfe is not null; --873109
--select * from sfe where sfe is not null and candidateID <10
--select sfe,count(*) as AMOUNT from sfe group by sfe Order By sfe

 
select top 10 * from attributes;
select top 10 * from attributeslink;

select distinct ltrim(rtrim(ATTDESCRIPTION)) from attributeslink
select distinct ATTOBJECTTYPE, count(*) from attributeslink group by ATTOBJECTTYPE
CAND	211648
COMP	76
CONT	3013
VAC	26973

-- COMPANY
with t as (
-- select top 10 * from CodeTables where Code = 'CONSU'
--select distinct c.BusinessType001, ct.Description
select c.UniqueID, ct.Description as industry
from clients c
left join CodeTables ct on ct.Code = c.BusinessType001
where ct.TabName = 'Bus Type' and c.UniqueID is not null and c.BusinessType001 <> '' --409
UNION ALL
-- select c.UniqueID, c.AccountName, al.*
select c.UniqueID, al.ATTDESCRIPTION as industry
-- select distinct al.ATTDESCRIPTION
from clients c
left join attributeslink al on al.ATTOBJECTUNIQ = c.UniqueID
where ATTOBJECTTYPE = 'COMP' and c.UniqueID is not null --76
)
select UniqueID,
        case industry
                When '.Net' then 28924
                When '1st Line' then 28925
                When 'Accountancy' then 28735
                When 'Admin and Secretarial ' then 28880
                When 'ADO.Net' then 28926
                When 'Advertising and PR' then 28732
                When 'Aerospace' then 28739
                When 'Agile' then 28927
                When 'AJAX' then 28928
                When 'Arts' then 28737
                When 'AS400' then 28929
                When 'ASP.Net' then 28930
                When 'Automated Testing' then 28931
                When 'Automotive' then 28738
                When 'Banking' then 28768
                When 'Building and Construction' then 28767
                When 'Business Analyst' then 28932
                When 'Business Intelligence' then 28933
                When 'C' then 28934
                When 'C#' then 28935
                When 'Call Centre' then 28907
                When 'Cisco' then 28936
                When 'Cisco (Gen)' then 28937
                When 'Cloud' then 28938
                When 'CMS (Gen)' then 28939
                When 'Cobol' then 28940
                When 'Construction' then 28908
                When 'Consultancy' then 28804
                When 'Consultancy / Technical Consultancy' then 28889
                When 'CRM' then 28941
                When 'Database Adminstrator' then 28942
                When 'Database Specialist' then 28943
                When 'Design and Creative' then 28883
                When 'Developer' then 28944
                When 'Developer/Programmer' then 28945
                When 'Digital' then 28946
                When 'Digital Agency' then 28890
                When 'eCommerce' then 28891
                When 'Education' then 28892
                When 'Electronics' then 28881
                When 'Engineering' then 28882
                When 'ERP' then 28947
                When 'Finance' then 28893
                When 'Financial Services' then 28776
                When 'FMCG' then 28813
                When 'Glovia' then 28948
                When 'Graduates and Trainees' then 28750
                When 'Health and Safety' then 28817
                When 'Hospital and Catering' then 28784
                When 'Human Resources and Personnel' then 28751
                When 'Insurance' then 28755
                When 'IT' then 28824
                When 'J2EE' then 28949
                When 'Java' then 28950
                When 'jQuery' then 28951
                When 'Lean/6 Sigma' then 28952
                When 'Legal ' then 28756
                When 'Leisure and Sport' then 28752
                When 'Logistics' then 28895
                When 'Logistics Distribution and Supply Chain' then 28760
                When 'Manufacturing' then 28896
                When 'Manufacturing and Production' then 28773
                When 'Marketing' then 28774
                When 'Media' then 28839
                When 'Medical and Nursing' then 28840
                When 'Microsoft & SQL Server' then 28954
                When 'Military' then 28790
                When 'Mining' then 28757
                When 'New Media and Internet' then 28770
                When 'NHS' then 28897
                When 'Not for Profit and Charities' then 28733
                When 'Oil and Gas' then 28769
                When 'Oil and Gas / Energy' then 28898
                When 'OO' then 28955
                When 'Oracle' then 28956
                When 'Pharma' then 28899
                When 'Pharmaceuticals' then 28758
                When 'Property and Housing' then 28850
                When 'Public' then 28900
                When 'Public Relations and Communications' then 28859
                When 'Public Safety' then 28860
                When 'Public Sector and Government' then 28851
                When 'Purchasing and Procurement' then 28852
                When 'Real Estate and Property' then 28853
                When 'Recruitment Consultancy' then 28854
                When 'Retail' then 28780
                When 'Sales' then 28868
                When 'Salesforce' then 28958
                When 'Science and Research' then 28764
                When 'Senior Appointments' then 28869
                When 'Sittikarn' then 28959
                When 'Social Care' then 28870
                When 'Software' then 28902
                When 'Software Developer' then 28960
                When 'SQL' then 28961
                When 'SQL Server' then 28962
                When 'SQL Server 2000' then 28963
                When 'Storage' then 28964
                When 'Supermarkets' then 28873
                When 'Systems Analyst' then 28965
                When 'Systems Testing' then 28966
                When 'T-SQL' then 28968
                When 'Telecommunications' then 28763
                When 'Tester' then 28967
                When 'Third Sector / Charity' then 28903
                When 'Trade and Services' then 28872
                When 'Transport and Rail' then 28796
                When 'Travel and Tourism' then 28875
                When 'UI' then 28969
                When 'Unit Testing' then 28970
                When 'Utilities' then 28778
                When 'Virtualisation' then 28971
                When 'Visual Basic' then 28972
                When 'WCF' then 28973
                When 'Windows Server' then 28974
                When 'WPF' then 28975
                When 'XML' then 28976
                When 'XPATH' then 28977
                When 'XSLT' then 28978
        else '' end as industry
        from t


-- CONTACT
with t as (
        -- select count(*)
        -- select distinct al.ATTDESCRIPTION
        --select c.ContactUniqueID, al.ATTDESCRIPTION as fe
        select concat(c.ContactUniqueID,'con') as ContactUniqueID, al.ATTDESCRIPTION as fe
        from contacts c
        left join attributeslink al on al.ATTOBJECTUNIQ = c.ContactUniqueID 
        where c.ContactUniqueID is not null and al.ATTOBJECTTYPE = 'CONT'
        )
select ContactUniqueID,
        case fe
When '.Net' then 4073
When '.Net 2.0' then 4074
When '.Net 3.0' then 3688
When '.Net 3.5' then 4075
When '.Net 4.0' then 3699
When '.Net mobile' then 3701
When '1st Line' then 3689
When '2G' then 3700
When '2nd Line' then 3827
When '3G' then 3828
When '3rd Line' then 3829
When 'Ab Initio' then 3702
When 'Aberdeen' then 4076
When 'Access' then 3690
When 'Account Manager' then 3830
When 'Accounting & Finance' then 3093
When 'Actionscript 2.0' then 3831
When 'Actionscript 3.0' then 4077
When 'Active Directory' then 3832
When 'Actuate' then 3691
When 'ADA' then 3833
When 'ADF' then 3834
When 'ADO.Net' then 4078
When 'Adobe Flash' then 3835
When 'Adobe Flex' then 3973
When 'Agile' then 3692
When 'Agresso' then 3836
When 'AIX' then 3693
When 'AJAX' then 4079
When 'Alteryx' then 3974
When 'Analogue' then 3837
When 'Android' then 3975
When 'Apple' then 3694
When 'Application Support' then 3838
When 'Architect' then 3695
When 'Arcplan' then 3696
When 'AS400' then 3697
When 'ASP.Net' then 3839
When 'Automated Testing' then 3698
When 'Automotive / Automotive Parts' then 3094
When 'AV' then 3703
When 'Azure' then 3704
When 'Baan' then 4080
When 'Back Up' then 3976
When 'Banking & Finance' then 3095
When 'Bash' then 4081
When 'Benjaporn' then 3840
When 'BI Consultant' then 3841
When 'Birst' then 3842
When 'Biz Dev' then 3843
When 'Blackberry' then 3705
When 'Board International' then 4082
When 'Borders' then 3844
When 'Bsm' then 3706
When 'Bus. Intelligence' then 3845
When 'Business Analyst' then 3977
When 'Business Development / Sales' then 3707
When 'Business Intelligence' then 3708
When 'Business Objects' then 3846
When 'C' then 3978
When 'C#' then 3979
When 'C++' then 4083
When 'CakePHP' then 3980
When 'Call Centre' then 3709
When 'Cantonese' then 3981
When 'CAT5' then 3847
When 'CAT6' then 4084
When 'CCA' then 3848
When 'CCDP' then 3849
When 'CCEE' then 4085
When 'CCI' then 3850
When 'CCIA' then 3982
When 'CCIE' then 3983
When 'CCNA' then 3710
When 'CCNP' then 3984
When 'CCSP' then 3711
When 'CEO' then 3712
When 'Chaiwat' then 4086
When 'Change' then 4087
When 'Change Analyst' then 3851
When 'Change Manager' then 3713
When 'Chemical / Material' then 3096
When 'Chief Financial Officer' then 3852
When 'Chief Operations Officer' then 3714
When 'Chief Technical Officer' then 4088
When 'CIO' then 3715
When 'CIO / CTO' then 3985
When 'Cisco' then 3986
When 'Cisco (Gen)' then 4089
When 'CISM' then 3853
When 'CISSP' then 3987
When 'Citirx (Gen)' then 3716
When 'Citrix' then 4090
When 'Classic ASP' then 3717
When 'Clojure' then 3988
When 'Cloud' then 3718
When 'CMOS' then 3989
When 'CMS (Gen)' then 3719
When 'Cobol' then 4091
When 'Cognos' then 3854
When 'Coins' then 4092
When 'ColdFusion' then 3855
When 'Construction' then 3990
When 'Council' then 3991
When 'CRM' then 3992
When 'Crystal Reports' then 3720
When 'CS Core' then 3856
When 'CSS' then 3857
When 'CSS3' then 4093
When 'Cucumber' then 4094
When 'Data Science' then 3993
When 'Database Adminstrator' then 3858
When 'Database Developer' then 3994
When 'Database Specialist' then 3859
When 'Databases' then 4095
When 'DataCentre' then 3995
When 'Datastage' then 3721
When 'Datawarehouse' then 4096
When 'DB2' then 4097
When 'Delphi' then 4098
When 'Developer' then 3860
When 'Developer/Programmer' then 4099
When 'Development' then 3722
When 'Development Manager' then 3861
When 'DHTML' then 3723
When 'DHTMLX' then 3862
When 'Digital' then 3996
When 'Director' then 3997
When 'DirectX' then 3724
When 'Discoverer' then 4100
When 'Django' then 3725
When 'Document Controller' then 4101
When 'DOJO' then 4102
When 'Dplyr' then 4103
When 'Drupal' then 3726
When 'Dundee' then 3863
When 'Dynamics' then 3864
When 'Dynamics (Gen)' then 3727
When 'Dynamics Ax' then 3998
When 'Dynamics Crm' then 3728
When 'Dynamics Gp' then 3999
When 'Dynamics Nav' then 3729
When 'Dynamics Sl' then 3730
When 'Dynatrace' then 4000
When 'Eclipse' then 3865
When 'Edinburgh' then 3731
When 'efacs' then 4001
When 'EJB' then 3732
When 'Elastic Search' then 3866
When 'Embedded C' then 4104
When 'Embedded C++' then 3733
When 'Engineering' then 3097
When 'Epicor' then 3734
When 'Ericsson' then 3867
When 'ERP' then 4105
When 'ERP Consultant' then 3735
When 'Essbase' then 4002
When 'ETL' then 3736
When 'Excel' then 4003
When 'Exchange' then 3868
When 'Executive Management' then 3098
When 'EXTGWT' then 3737
When 'EXTJS' then 4004
When 'F#' then 3738
When 'Fedora' then 4106
When 'Fife' then 4107
When 'Financial Director' then 4108
When 'Firewall' then 4005
When 'FMCG' then 3869
When 'FMEA' then 4109
When 'Forms' then 4006
When 'Fortran' then 4007
When 'French' then 3870
When 'Front End Web' then 3739
When 'Functional Testing' then 4008
When 'Games Developer' then 3871
When 'German' then 4110
When 'GIS' then 4111
When 'Glasgow' then 3740
When 'Glovia' then 4009
When 'Gnu' then 3741
When 'Government' then 4010
When 'Group Policy' then 4112
When 'Groupwise' then 4011
When 'Hadoop' then 3872
When 'Handheld' then 3742
When 'HAZOP' then 4113
When 'Helpdesk Manager' then 3743
When 'Hibernate' then 4012
When 'HP-UX' then 4115
When 'HPUX' then 4114
When 'HR' then 4116
When 'HR, GA & Facilities' then 3099
When 'HSE' then 3744
When 'HTML' then 4013
When 'HTML5' then 4117
When 'Human Resource(sample only)' then 2981
When 'Hyperion' then 4118
When 'IBM' then 4014
When 'IFS' then 4119
When 'IIS' then 3745
When 'Industrial Equipment / Plant / Parts' then 3101
When 'Infor' then 4015
When 'Informatica' then 3873
When 'Infrastructure' then 3874
When 'Infrastructure Manager' then 3875
When 'Infrastructure Project Manager' then 3876
When 'Ingress' then 3746
When 'Interest In Big Data' then 3877
When 'iPhone' then 3878
When 'Ipython' then 4016
When 'ISO20071' then 3879
When 'IT & Telecoms' then 3100
When 'IT Director' then 3880
When 'IT Manager' then 4017
When 'Italian' then 3747
When 'Iterative' then 4120
When 'ITIL' then 3748
When 'J2EE' then 3881
When 'J2ME' then 3882
When 'J2SE' then 3749
When 'Jaspersoft' then 3750
When 'Java' then 3883
When 'JavaScript' then 3751
When 'JBoss' then 3884
When 'Jd Edwards E1' then 4018
When 'JD Edwards World' then 4019
When 'JDBC' then 3752
When 'JDeveloper' then 4121
When 'Jidapa' then 3753
When 'Joomla' then 3754
When 'jQuery' then 3885
When 'JSF' then 4122
When 'JSP' then 4020
When 'JSPHP' then 3886
When 'JUnit' then 4123
When 'JWT' then 3755
When 'Jython' then 4124
When 'Kernel' then 3756
When 'Kimball' then 3757
When 'Knitr' then 3887
When 'Korn Shell' then 3758
When 'Lamp' then 3759
When 'LAN' then 4125
When 'Languages' then 4021
When 'Lean/6 Sigma' then 4126
When 'Legal' then 3102
When 'LINQ' then 4022
When 'Linux' then 3888
When 'Load testing' then 3889
When 'LoadRunner' then 3760
When 'Logistics' then 3890
When 'Lotus' then 3891
When 'Lotus Domino' then 3892
When 'LTE' then 4023
When 'Lync' then 3893
When 'Magento' then 3894
When 'Mahout' then 4024
When 'MAN' then 3895
When 'Manager' then 3761
When 'Managing Director' then 3896
When 'Mandarin' then 3897
When 'Manual Testing' then 3762
When 'Manufacturing' then 3898
When 'Mapics' then 3763
When 'Masters  Level' then 3899
When 'Matlab' then 3764
When 'Maven' then 4025
When 'Maximo' then 3900
When 'mba' then 3765
When 'MCDBA' then 4026
When 'Mcdoogs' then 3901
When 'MCITP' then 3902
When 'MCP' then 3903
When 'MCPD' then 3904
When 'MCSE' then 3905
When 'MCTP' then 3906
When 'MCTS' then 4027
When 'Medical Device / Diagnostics / Analytical' then 3103
When 'Mercury' then 3766
When 'Microsoft' then 4028
When 'Microsoft & SQL Server' then 3907
When 'Microsoft BI' then 4127
When 'Microsoft Partner' then 3908
When 'Microstrategy' then 3909
When 'Microwave' then 3910
When 'Mobile' then 3911
When 'Mobile Development' then 4128
When 'Monitoring' then 4129
When 'MooTools' then 4130
When 'Movex' then 4131
When 'MRP' then 4029
When 'Ms Bi Developer' then 4030
When 'Mura' then 3912
When 'Music' then 2982
When 'MVC' then 4132
When 'MySQL' then 4031
When 'NetAct' then 4133
When 'NetBeans' then 3913
When 'Netezza' then 3767
When 'NetScaler' then 4134
When 'Netsuite' then 3914
When 'Netware' then 3768
When 'Network Engineer' then 4032
When 'Network Manager' then 4033
When 'Networking' then 4135
When 'nHibernate' then 4034
When 'NHS' then 3769
When 'Nosql' then 3770
When 'Novell (Gen)' then 3771
When 'NSN' then 4136
When 'OBIEE' then 3915
When 'Ods' then 4035
When 'Oil And Gas' then 4137
When 'Olap' then 4138
When 'Oltp' then 3916
When 'OO' then 4139
When 'OpenSUSE' then 4036
When 'Operations Manager' then 4037
When 'Oracle' then 4038
When 'Oracle 10g' then 4140
When 'Oracle 11g' then 4141
When 'Oracle 11gR2' then 4142
When 'Oracle 8i' then 3772
When 'Oracle 9i' then 3773
When 'Oracle Application Server' then 3774
When 'Oracle BI' then 3917
When 'Oracle Collaboration' then 3918
When 'Oracle Crm' then 3919
When 'Oracle DataGuard' then 3920
When 'Oracle Designer' then 3775
When 'Oracle Dev (Gen)' then 4143
When 'Oracle ERP' then 3921
When 'Oracle Flashback' then 4144
When 'Oracle Lite' then 3922
When 'Oracle OCA' then 4145
When 'Oracle OCP' then 3923
When 'Oracle OEM' then 4039
When 'Oracle RAC' then 3924
When 'Oracle RMAN' then 3925
When 'OSS' then 4040
When 'Outlook' then 3926
When 'OWB' then 3927
When 'Panda' then 4041
When 'Panorama' then 4146
When 'Pascal' then 3776
When 'Pegasus' then 3777
When 'Pentaho' then 3778
When 'PeopleSoft' then 3928
When 'Performance Point' then 3929
When 'Performance Testing' then 4147
When 'Perl' then 4148
When 'Perth' then 4149
When 'Pharawi' then 3930
When 'Pharma' then 4150
When 'Pharmaceutical / CRO / Reagents / Personal Care' then 3104
When 'PHD Level' then 3931
When 'PHP' then 3779
When 'PHP (General)' then 4151
When 'PHP5' then 3780
When 'PL/SQL' then 3932
When 'PMO' then 4152
When 'Prairie' then 4153
When 'Process Safety' then 3781
When 'Procurement' then 3933
When 'Product Manager' then 4042
When 'Prognoz' then 3782
When 'Programme Manager' then 4154
When 'Progress' then 3934
When 'Project Manager' then 3783
When 'PS Core' then 4155
When 'Publisher' then 3935
When 'Python' then 3784
When 'QA' then 3785
When 'Qlikview' then 4156
When 'QTP' then 3936
When 'RAD' then 3786
When 'Radio' then 3787
When 'RCN' then 3937
When 'Real Estate & Insurance' then 3105
When 'RedHat' then 3788
When 'Regression Testing' then 4043
When 'Reports' then 3938
When 'Retail' then 4157
When 'RF' then 3939
When 'Ruby' then 3789
When 'Ruby on Rails' then 4158
When 'RUP' then 3790
When 'Russian' then 3940
When 'Sage' then 3791
When 'Sales & Marketing' then 3106
When 'Salesforce' then 4159
When 'Salient' then 4044
When 'Samba' then 4160
When 'SAP' then 3792
When 'SAP ABAP' then 3793
When 'SAP BI' then 4161
When 'Sap Crm' then 4045
When 'SAP CRM (Gen)' then 4046
When 'SAP CRM Interaction Centre' then 4162
When 'SAP CRM Marketing' then 3941
When 'SAP CRM Web' then 3794
When 'SAP FICO' then 3795
When 'SAP Financials' then 4163
When 'SAP HR' then 4047
When 'SAP MM' then 3942
When 'SAP PP' then 3943
When 'SAP QM' then 3944
When 'SAS' then 3796
When 'SBS' then 4164
When 'Scala' then 4165
When 'SCCM' then 3945
When 'Script.aculo.us' then 4048
When 'Scrum' then 3946
When 'Security Testing' then 4049
When 'Selenium' then 4050
When 'Semiconductor / Embedded Device' then 3107
When 'Server Engineer' then 4051
When 'Server Support' then 3947
When 'Service Delivery Manager' then 4166
When 'Servlets' then 3948
When 'Sharepoint' then 3797
When 'Shell Script' then 3949
When 'Siebel' then 4167
When 'Siebel BI' then 4168
When 'Silverlight' then 4169
When 'SiteCore' then 3798
When 'Sittikarn' then 4052
When 'Software' then 3950
When 'Software Developer' then 3951
When 'Software Project Manager' then 3952
When 'Solaris' then 3799
When 'Spanish' then 3953
When 'Spring' then 4170
When 'SQL' then 4171
When 'SQL Server' then 3800
When 'SQL Server 2000' then 3954
When 'SQL Server 2005' then 3801
When 'SQL Server 2008' then 4053
When 'SQL Server 2012' then 4054
When 'SQL Server 2014' then 4055
When 'SQL Server Compact' then 4172
When 'SSAS' then 4173
When 'SSIS' then 3802
When 'SSRS' then 4056
When 'Stirling' then 4174
When 'Storage' then 4057
When 'Struts' then 4058
When 'Sugar' then 4175
When 'Sun Accounts' then 3955
When 'Sun Certified' then 3803
When 'Supply Chain Management' then 3108
When 'Swing' then 3804
When 'Sybase' then 3805
When 'Symfony' then 3806
When 'Syspro' then 4059
When 'Systems Analyst' then 3807
When 'Systems Testing' then 4176
When 'T-SQL' then 4181
When 'Tableau' then 4060
When 'Tapestry' then 3956
When 'Targit' then 4061
When 'TCP/IP' then 3957
When 'Team Leader' then 3808
When 'Technical Consultancy' then 3809
When 'Test Director' then 4062
When 'Test Manager' then 4177
When 'Tester' then 4178
When 'Testing' then 3810
When 'TETRA Telecoms' then 3811
When 'TFS' then 4179
When 'Third Sector' then 3812
When 'Tibco' then 4180
When 'Tomcat' then 3958
When 'Traffic' then 4063
When 'Trainer' then 4064
When 'UAT' then 4182
When 'UI' then 3959
When 'UMTS' then 4065
When 'Unit Testing' then 4183
When 'Unix (Gen)' then 3960
When 'Unix Dev (Gen)' then 4184
When 'UNIX/LINUX' then 4185
When 'Utilities' then 4186
When 'UTRAN' then 3813
When 'UX' then 4066
When 'VB.Net' then 4067
When 'VBA' then 3961
When 'Verilog' then 3962
When 'Virtualisation' then 3814
When 'Visual Basic' then 3963
When 'VMODEL' then 4187
When 'Voice' then 3815
When 'VPN' then 3964
When 'W3C' then 3816
When 'Wamp' then 3817
When 'WAN' then 4068
When 'Waterfall' then 3965
When 'WCF' then 4188
When 'Web Designer' then 3818
When 'WF' then 3819
When 'WIFI' then 3966
When 'WiMAX' then 3967
When 'Windows Desktop' then 4189
When 'Windows Server' then 3820
When 'WLAN' then 3821
When 'WordPress' then 3968
When 'WPF' then 3822
When 'Xamarin' then 3823
When 'XAML' then 3969
When 'XenApp' then 4190
When 'Xendesktop' then 3824
When 'XenServer' then 4069
When 'XHTML' then 4070
When 'XML' then 3825
When 'XPATH' then 3826
When 'XQuery' then 4071
When 'XSL' then 3970
When 'XSLT' then 4072
When 'Yellowfin' then 4191
When 'YUI' then 3971
When 'Zend' then 3972
else '' end fe
from t
where ContactUniqueID in ('10019con','10102con','10534con','10544con','10593con','10054con','10060con','10098con','10100con','10061con','10104con','10195con','1031con','10394con','10553con','10103con','10326con','10393con','10417con','10561con','10569con','10622con','10675con','10841con','10846con','10880con','10632con','10967con','11150con','11213con','10663con','10728con','10731con','10736con','1080con','10863con','11037con','10954con','10968con','11059con','11154con','11063con','11114con','11124con','11223con','1128con','11175con','11350con','11412con','11436con','11624con','11232con','11254con','11257con','11301con','11414con','11307con','11534con','11549con','1164con','115con','11540con','11622con','1165con','11631con','11752con','11655con','11880con','12112con','11714con','11922con','11899con','11905con','11925con','12101con','11926con','1194con','11946con','12024con','12120con','12121con','12184con','12187con','12204con','12244con','12230con','12379con','12300con','12340con','12318con','12386con','12405con','12422con','12505con','12460con','12489con','1260con','1327con','1389con','1399con','1668con','1719con','1447con','1453con','1449con','1463con','1523con','1465con','1552con','1636con','1673con','1750con','1943con','1678con','1871con','1840con','1888con','1941con','1990con','1979con','2055con','2094con','1984con','2005con','2014con','2020con','2040con','2069con','2201con','2250con','2187con','2210con','2214con','2188con','2329con','2318con','2470con','2543con','2341con','2391con','2372con','2422con','243con','2463con','2471con','2480con','2509con','2514con','2557con','2567con','2644con','2658con','2670con','2671con','2707con','2765con','2970con','2709con','2769con','2806con','281con','3402con','2841con','3415con','347con','3472con','3482con','3511con','3495con','3555con','3553con','3557con','3646con','3577con','3594con','3607con','3651con','370con','373con','3752con','4176con','3771con','3993con','3779con','3995con','4039con','4261con','410con','4195con','4231con','4219con','4277con','4259con','4289con','4306con','4287con','4609con','4650con','4288con','4324con','4350con','4348con','4364con','4771con','458con','4755con','4777con','476con','4798con','481con','4839con','513con','4967con','5096con','510con','5104con','5150con','511con','512con','515con','5178con','5253con','5156con','5274con','5325con','5241con','5316con','5440con','5396con','5417con','5449con','5579con','5450con','5621con','5627con','551con','5629con','5586con','5594con','560con','5647con','5638con','5810con','5906con','5742con','5765con','5851con','5966con','5904con','5950con','5958con','5995con','5977con','5993con','6023con','6041con','6304con','6065con','6074con','6097con','6115con','6131con','6144con','6184con','6173con','6236con','6294con','6303con','6495con','6308con','6608con','637con','6502con','6628con','6643con','6668con','6675con','6847con','6724con','6729con','6794con','6799con','7417con','7593con','6837con','7016con','7055con','7067con','7100con','7237con','7116con','7160con','721con','7225con','7321con','7332con','7372con','7401con','7416con','7421con','7523con','7535con','7596con','7554con','7626con','7597con','7628con','7897con','7944con','7634con','7766con','7654con','7663con','7958con','7849con','7990con','8024con','8262con','8333con','8057con','8179con','8100con','8132con','8299con','8207con','8240con','8248con','8331con','8347con','8436con','8456con','8522con','8570con','8552con','8631con','8646con','865con','869con','8737con','8801con','8959con','8741con','8833con','8991con','8994con','9329con','8998con','9001con','906con','9008con','9219con','909con','918con','9226con','9293con','9340con','9344con','9405con','9356con','9444con','9412con','9421con','9413con','944con','9469con','9480con','9483con','9518con','9481con','9498con','9503con','9599con','9538con','9607con','9547con','9567con','9679con','9619con','9637con','9658con','9641con','9672con','9693con','9784con','9719con','9825con','9828con','9972con','9858con','9878con','9889con','9942con','9943con','9977con','9992con','9996con')

/*
select al.ATTOBJECTUNIQ, al.ATTDESCRIPTION, a.LongDescription
from attributeslink al
left join attributes a on al.ATTRIBUTEUNIQ = a.AttributeUniq
left join contacts c on al.ATTOBJECTUNIQ = c.ContactUniqueID
where ATTOBJECTTYPE = 'CONT' --and al.ATTDESCRIPTION != a.LongDescription
*/

-- CANDIDATE
with t as (
        -- select count(*)
        -- select distinct al.ATTDESCRIPTION
        select c.UniqueID, al.ATTDESCRIPTION as fe
        from candidates c
        left join attributeslink al on al.ATTOBJECTUNIQ = c.UniqueID
        where c.UniqueID is not null and al.ATTOBJECTTYPE = 'CAND'
        )
--select count(*) from t
select top 200 UniqueID
        , case fe
When '.Net' then 4073
When '.Net 2.0' then 4074
When '.Net 3.0' then 3688
When '.Net 3.5' then 4075
When '.Net 4.0' then 3699
When '.Net mobile' then 3701
When '1st Line' then 3689
When '2G' then 3700
When '2nd Line' then 3827
When '3G' then 3828
When '3rd Line' then 3829
When 'Ab Initio' then 3702
When 'Aberdeen' then 4076
When 'Access' then 3690
When 'Account Manager' then 3830
When 'Accounting & Finance' then 3093
When 'Actionscript 2.0' then 3831
When 'Actionscript 3.0' then 4077
When 'Active Directory' then 3832
When 'Actuate' then 3691
When 'ADA' then 3833
When 'ADF' then 3834
When 'ADO.Net' then 4078
When 'Adobe Flash' then 3835
When 'Adobe Flex' then 3973
When 'Agile' then 3692
When 'Agresso' then 3836
When 'AIX' then 3693
When 'AJAX' then 4079
When 'Alteryx' then 3974
When 'Analogue' then 3837
When 'Android' then 3975
When 'Apple' then 3694
When 'Application Support' then 3838
When 'Architect' then 3695
When 'Arcplan' then 3696
When 'AS400' then 3697
When 'ASP.Net' then 3839
When 'Automated Testing' then 3698
When 'Automotive / Automotive Parts' then 3094
When 'AV' then 3703
When 'Azure' then 3704
When 'Baan' then 4080
When 'Back Up' then 3976
When 'Banking & Finance' then 3095
When 'Bash' then 4081
When 'Benjaporn' then 3840
When 'BI Consultant' then 3841
When 'Birst' then 3842
When 'Biz Dev' then 3843
When 'Blackberry' then 3705
When 'Board International' then 4082
When 'Borders' then 3844
When 'Bsm' then 3706
When 'Bus. Intelligence' then 3845
When 'Business Analyst' then 3977
When 'Business Development / Sales' then 3707
When 'Business Intelligence' then 3708
When 'Business Objects' then 3846
When 'C' then 3978
When 'C#' then 3979
When 'C++' then 4083
When 'CakePHP' then 3980
When 'Call Centre' then 3709
When 'Cantonese' then 3981
When 'CAT5' then 3847
When 'CAT6' then 4084
When 'CCA' then 3848
When 'CCDP' then 3849
When 'CCEE' then 4085
When 'CCI' then 3850
When 'CCIA' then 3982
When 'CCIE' then 3983
When 'CCNA' then 3710
When 'CCNP' then 3984
When 'CCSP' then 3711
When 'CEO' then 3712
When 'Chaiwat' then 4086
When 'Change' then 4087
When 'Change Analyst' then 3851
When 'Change Manager' then 3713
When 'Chemical / Material' then 3096
When 'Chief Financial Officer' then 3852
When 'Chief Operations Officer' then 3714
When 'Chief Technical Officer' then 4088
When 'CIO' then 3715
When 'CIO / CTO' then 3985
When 'Cisco' then 3986
When 'Cisco (Gen)' then 4089
When 'CISM' then 3853
When 'CISSP' then 3987
When 'Citirx (Gen)' then 3716
When 'Citrix' then 4090
When 'Classic ASP' then 3717
When 'Clojure' then 3988
When 'Cloud' then 3718
When 'CMOS' then 3989
When 'CMS (Gen)' then 3719
When 'Cobol' then 4091
When 'Cognos' then 3854
When 'Coins' then 4092
When 'ColdFusion' then 3855
When 'Construction' then 3990
When 'Council' then 3991
When 'CRM' then 3992
When 'Crystal Reports' then 3720
When 'CS Core' then 3856
When 'CSS' then 3857
When 'CSS3' then 4093
When 'Cucumber' then 4094
When 'Data Science' then 3993
When 'Database Adminstrator' then 3858
When 'Database Developer' then 3994
When 'Database Specialist' then 3859
When 'Databases' then 4095
When 'DataCentre' then 3995
When 'Datastage' then 3721
When 'Datawarehouse' then 4096
When 'DB2' then 4097
When 'Delphi' then 4098
When 'Developer' then 3860
When 'Developer/Programmer' then 4099
When 'Development' then 3722
When 'Development Manager' then 3861
When 'DHTML' then 3723
When 'DHTMLX' then 3862
When 'Digital' then 3996
When 'Director' then 3997
When 'DirectX' then 3724
When 'Discoverer' then 4100
When 'Django' then 3725
When 'Document Controller' then 4101
When 'DOJO' then 4102
When 'Dplyr' then 4103
When 'Drupal' then 3726
When 'Dundee' then 3863
When 'Dynamics' then 3864
When 'Dynamics (Gen)' then 3727
When 'Dynamics Ax' then 3998
When 'Dynamics Crm' then 3728
When 'Dynamics Gp' then 3999
When 'Dynamics Nav' then 3729
When 'Dynamics Sl' then 3730
When 'Dynatrace' then 4000
When 'Eclipse' then 3865
When 'Edinburgh' then 3731
When 'efacs' then 4001
When 'EJB' then 3732
When 'Elastic Search' then 3866
When 'Embedded C' then 4104
When 'Embedded C++' then 3733
When 'Engineering' then 3097
When 'Epicor' then 3734
When 'Ericsson' then 3867
When 'ERP' then 4105
When 'ERP Consultant' then 3735
When 'Essbase' then 4002
When 'ETL' then 3736
When 'Excel' then 4003
When 'Exchange' then 3868
When 'Executive Management' then 3098
When 'EXTGWT' then 3737
When 'EXTJS' then 4004
When 'F#' then 3738
When 'Fedora' then 4106
When 'Fife' then 4107
When 'Financial Director' then 4108
When 'Firewall' then 4005
When 'FMCG' then 3869
When 'FMEA' then 4109
When 'Forms' then 4006
When 'Fortran' then 4007
When 'French' then 3870
When 'Front End Web' then 3739
When 'Functional Testing' then 4008
When 'Games Developer' then 3871
When 'German' then 4110
When 'GIS' then 4111
When 'Glasgow' then 3740
When 'Glovia' then 4009
When 'Gnu' then 3741
When 'Government' then 4010
When 'Group Policy' then 4112
When 'Groupwise' then 4011
When 'Hadoop' then 3872
When 'Handheld' then 3742
When 'HAZOP' then 4113
When 'Helpdesk Manager' then 3743
When 'Hibernate' then 4012
When 'HP-UX' then 4115
When 'HPUX' then 4114
When 'HR' then 4116
When 'HR, GA & Facilities' then 3099
When 'HSE' then 3744
When 'HTML' then 4013
When 'HTML5' then 4117
When 'Human Resource(sample only)' then 2981
When 'Hyperion' then 4118
When 'IBM' then 4014
When 'IFS' then 4119
When 'IIS' then 3745
When 'Industrial Equipment / Plant / Parts' then 3101
When 'Infor' then 4015
When 'Informatica' then 3873
When 'Infrastructure' then 3874
When 'Infrastructure Manager' then 3875
When 'Infrastructure Project Manager' then 3876
When 'Ingress' then 3746
When 'Interest In Big Data' then 3877
When 'iPhone' then 3878
When 'Ipython' then 4016
When 'ISO20071' then 3879
When 'IT & Telecoms' then 3100
When 'IT Director' then 3880
When 'IT Manager' then 4017
When 'Italian' then 3747
When 'Iterative' then 4120
When 'ITIL' then 3748
When 'J2EE' then 3881
When 'J2ME' then 3882
When 'J2SE' then 3749
When 'Jaspersoft' then 3750
When 'Java' then 3883
When 'JavaScript' then 3751
When 'JBoss' then 3884
When 'Jd Edwards E1' then 4018
When 'JD Edwards World' then 4019
When 'JDBC' then 3752
When 'JDeveloper' then 4121
When 'Jidapa' then 3753
When 'Joomla' then 3754
When 'jQuery' then 3885
When 'JSF' then 4122
When 'JSP' then 4020
When 'JSPHP' then 3886
When 'JUnit' then 4123
When 'JWT' then 3755
When 'Jython' then 4124
When 'Kernel' then 3756
When 'Kimball' then 3757
When 'Knitr' then 3887
When 'Korn Shell' then 3758
When 'Lamp' then 3759
When 'LAN' then 4125
When 'Languages' then 4021
When 'Lean/6 Sigma' then 4126
When 'Legal' then 3102
When 'LINQ' then 4022
When 'Linux' then 3888
When 'Load testing' then 3889
When 'LoadRunner' then 3760
When 'Logistics' then 3890
When 'Lotus' then 3891
When 'Lotus Domino' then 3892
When 'LTE' then 4023
When 'Lync' then 3893
When 'Magento' then 3894
When 'Mahout' then 4024
When 'MAN' then 3895
When 'Manager' then 3761
When 'Managing Director' then 3896
When 'Mandarin' then 3897
When 'Manual Testing' then 3762
When 'Manufacturing' then 3898
When 'Mapics' then 3763
When 'Masters  Level' then 3899
When 'Matlab' then 3764
When 'Maven' then 4025
When 'Maximo' then 3900
When 'mba' then 3765
When 'MCDBA' then 4026
When 'Mcdoogs' then 3901
When 'MCITP' then 3902
When 'MCP' then 3903
When 'MCPD' then 3904
When 'MCSE' then 3905
When 'MCTP' then 3906
When 'MCTS' then 4027
When 'Medical Device / Diagnostics / Analytical' then 3103
When 'Mercury' then 3766
When 'Microsoft' then 4028
When 'Microsoft & SQL Server' then 3907
When 'Microsoft BI' then 4127
When 'Microsoft Partner' then 3908
When 'Microstrategy' then 3909
When 'Microwave' then 3910
When 'Mobile' then 3911
When 'Mobile Development' then 4128
When 'Monitoring' then 4129
When 'MooTools' then 4130
When 'Movex' then 4131
When 'MRP' then 4029
When 'Ms Bi Developer' then 4030
When 'Mura' then 3912
When 'Music' then 2982
When 'MVC' then 4132
When 'MySQL' then 4031
When 'NetAct' then 4133
When 'NetBeans' then 3913
When 'Netezza' then 3767
When 'NetScaler' then 4134
When 'Netsuite' then 3914
When 'Netware' then 3768
When 'Network Engineer' then 4032
When 'Network Manager' then 4033
When 'Networking' then 4135
When 'nHibernate' then 4034
When 'NHS' then 3769
When 'Nosql' then 3770
When 'Novell (Gen)' then 3771
When 'NSN' then 4136
When 'OBIEE' then 3915
When 'Ods' then 4035
When 'Oil And Gas' then 4137
When 'Olap' then 4138
When 'Oltp' then 3916
When 'OO' then 4139
When 'OpenSUSE' then 4036
When 'Operations Manager' then 4037
When 'Oracle' then 4038
When 'Oracle 10g' then 4140
When 'Oracle 11g' then 4141
When 'Oracle 11gR2' then 4142
When 'Oracle 8i' then 3772
When 'Oracle 9i' then 3773
When 'Oracle Application Server' then 3774
When 'Oracle BI' then 3917
When 'Oracle Collaboration' then 3918
When 'Oracle Crm' then 3919
When 'Oracle DataGuard' then 3920
When 'Oracle Designer' then 3775
When 'Oracle Dev (Gen)' then 4143
When 'Oracle ERP' then 3921
When 'Oracle Flashback' then 4144
When 'Oracle Lite' then 3922
When 'Oracle OCA' then 4145
When 'Oracle OCP' then 3923
When 'Oracle OEM' then 4039
When 'Oracle RAC' then 3924
When 'Oracle RMAN' then 3925
When 'OSS' then 4040
When 'Outlook' then 3926
When 'OWB' then 3927
When 'Panda' then 4041
When 'Panorama' then 4146
When 'Pascal' then 3776
When 'Pegasus' then 3777
When 'Pentaho' then 3778
When 'PeopleSoft' then 3928
When 'Performance Point' then 3929
When 'Performance Testing' then 4147
When 'Perl' then 4148
When 'Perth' then 4149
When 'Pharawi' then 3930
When 'Pharma' then 4150
When 'Pharmaceutical / CRO / Reagents / Personal Care' then 3104
When 'PHD Level' then 3931
When 'PHP' then 3779
When 'PHP (General)' then 4151
When 'PHP5' then 3780
When 'PL/SQL' then 3932
When 'PMO' then 4152
When 'Prairie' then 4153
When 'Process Safety' then 3781
When 'Procurement' then 3933
When 'Product Manager' then 4042
When 'Prognoz' then 3782
When 'Programme Manager' then 4154
When 'Progress' then 3934
When 'Project Manager' then 3783
When 'PS Core' then 4155
When 'Publisher' then 3935
When 'Python' then 3784
When 'QA' then 3785
When 'Qlikview' then 4156
When 'QTP' then 3936
When 'RAD' then 3786
When 'Radio' then 3787
When 'RCN' then 3937
When 'Real Estate & Insurance' then 3105
When 'RedHat' then 3788
When 'Regression Testing' then 4043
When 'Reports' then 3938
When 'Retail' then 4157
When 'RF' then 3939
When 'Ruby' then 3789
When 'Ruby on Rails' then 4158
When 'RUP' then 3790
When 'Russian' then 3940
When 'Sage' then 3791
When 'Sales & Marketing' then 3106
When 'Salesforce' then 4159
When 'Salient' then 4044
When 'Samba' then 4160
When 'SAP' then 3792
When 'SAP ABAP' then 3793
When 'SAP BI' then 4161
When 'Sap Crm' then 4045
When 'SAP CRM (Gen)' then 4046
When 'SAP CRM Interaction Centre' then 4162
When 'SAP CRM Marketing' then 3941
When 'SAP CRM Web' then 3794
When 'SAP FICO' then 3795
When 'SAP Financials' then 4163
When 'SAP HR' then 4047
When 'SAP MM' then 3942
When 'SAP PP' then 3943
When 'SAP QM' then 3944
When 'SAS' then 3796
When 'SBS' then 4164
When 'Scala' then 4165
When 'SCCM' then 3945
When 'Script.aculo.us' then 4048
When 'Scrum' then 3946
When 'Security Testing' then 4049
When 'Selenium' then 4050
When 'Semiconductor / Embedded Device' then 3107
When 'Server Engineer' then 4051
When 'Server Support' then 3947
When 'Service Delivery Manager' then 4166
When 'Servlets' then 3948
When 'Sharepoint' then 3797
When 'Shell Script' then 3949
When 'Siebel' then 4167
When 'Siebel BI' then 4168
When 'Silverlight' then 4169
When 'SiteCore' then 3798
When 'Sittikarn' then 4052
When 'Software' then 3950
When 'Software Developer' then 3951
When 'Software Project Manager' then 3952
When 'Solaris' then 3799
When 'Spanish' then 3953
When 'Spring' then 4170
When 'SQL' then 4171
When 'SQL Server' then 3800
When 'SQL Server 2000' then 3954
When 'SQL Server 2005' then 3801
When 'SQL Server 2008' then 4053
When 'SQL Server 2012' then 4054
When 'SQL Server 2014' then 4055
When 'SQL Server Compact' then 4172
When 'SSAS' then 4173
When 'SSIS' then 3802
When 'SSRS' then 4056
When 'Stirling' then 4174
When 'Storage' then 4057
When 'Struts' then 4058
When 'Sugar' then 4175
When 'Sun Accounts' then 3955
When 'Sun Certified' then 3803
When 'Supply Chain Management' then 3108
When 'Swing' then 3804
When 'Sybase' then 3805
When 'Symfony' then 3806
When 'Syspro' then 4059
When 'Systems Analyst' then 3807
When 'Systems Testing' then 4176
When 'T-SQL' then 4181
When 'Tableau' then 4060
When 'Tapestry' then 3956
When 'Targit' then 4061
When 'TCP/IP' then 3957
When 'Team Leader' then 3808
When 'Technical Consultancy' then 3809
When 'Test Director' then 4062
When 'Test Manager' then 4177
When 'Tester' then 4178
When 'Testing' then 3810
When 'TETRA Telecoms' then 3811
When 'TFS' then 4179
When 'Third Sector' then 3812
When 'Tibco' then 4180
When 'Tomcat' then 3958
When 'Traffic' then 4063
When 'Trainer' then 4064
When 'UAT' then 4182
When 'UI' then 3959
When 'UMTS' then 4065
When 'Unit Testing' then 4183
When 'Unix (Gen)' then 3960
When 'Unix Dev (Gen)' then 4184
When 'UNIX/LINUX' then 4185
When 'Utilities' then 4186
When 'UTRAN' then 3813
When 'UX' then 4066
When 'VB.Net' then 4067
When 'VBA' then 3961
When 'Verilog' then 3962
When 'Virtualisation' then 3814
When 'Visual Basic' then 3963
When 'VMODEL' then 4187
When 'Voice' then 3815
When 'VPN' then 3964
When 'W3C' then 3816
When 'Wamp' then 3817
When 'WAN' then 4068
When 'Waterfall' then 3965
When 'WCF' then 4188
When 'Web Designer' then 3818
When 'WF' then 3819
When 'WIFI' then 3966
When 'WiMAX' then 3967
When 'Windows Desktop' then 4189
When 'Windows Server' then 3820
When 'WLAN' then 3821
When 'WordPress' then 3968
When 'WPF' then 3822
When 'Xamarin' then 3823
When 'XAML' then 3969
When 'XenApp' then 4190
When 'Xendesktop' then 3824
When 'XenServer' then 4069
When 'XHTML' then 4070
When 'XML' then 3825
When 'XPATH' then 3826
When 'XQuery' then 4071
When 'XSL' then 3970
When 'XSLT' then 4072
When 'Yellowfin' then 4191
When 'YUI' then 3971
When 'Zend' then 3972
else '' end fe
from t




-- JOB
with t as (
        -- select count(*)
        -- select distinct al.ATTDESCRIPTION
        select c.UniqueID,c.RoleDescription, al.ATTDESCRIPTION as fe
        from vacancies c
        left join attributeslink al on al.ATTOBJECTUNIQ = c.UniqueID 
        where c.UniqueID is not null and al.ATTOBJECTTYPE = 'VAC'
        )
select count(*) from t
select top 200 UniqueID , RoleDescription
        , case fe
When '.Net' then 4073
When '.Net 2.0' then 4074
When '.Net 3.0' then 3688
When '.Net 3.5' then 4075
When '.Net 4.0' then 3699
When '.Net mobile' then 3701
When '1st Line' then 3689
When '2G' then 3700
When '2nd Line' then 3827
When '3G' then 3828
When '3rd Line' then 3829
When 'Ab Initio' then 3702
When 'Aberdeen' then 4076
When 'Access' then 3690
When 'Account Manager' then 3830
When 'Accounting & Finance' then 3093
When 'Actionscript 2.0' then 3831
When 'Actionscript 3.0' then 4077
When 'Active Directory' then 3832
When 'Actuate' then 3691
When 'ADA' then 3833
When 'ADF' then 3834
When 'ADO.Net' then 4078
When 'Adobe Flash' then 3835
When 'Adobe Flex' then 3973
When 'Agile' then 3692
When 'Agresso' then 3836
When 'AIX' then 3693
When 'AJAX' then 4079
When 'Alteryx' then 3974
When 'Analogue' then 3837
When 'Android' then 3975
When 'Apple' then 3694
When 'Application Support' then 3838
When 'Architect' then 3695
When 'Arcplan' then 3696
When 'AS400' then 3697
When 'ASP.Net' then 3839
When 'Automated Testing' then 3698
When 'Automotive / Automotive Parts' then 3094
When 'AV' then 3703
When 'Azure' then 3704
When 'Baan' then 4080
When 'Back Up' then 3976
When 'Banking & Finance' then 3095
When 'Bash' then 4081
When 'Benjaporn' then 3840
When 'BI Consultant' then 3841
When 'Birst' then 3842
When 'Biz Dev' then 3843
When 'Blackberry' then 3705
When 'Board International' then 4082
When 'Borders' then 3844
When 'Bsm' then 3706
When 'Bus. Intelligence' then 3845
When 'Business Analyst' then 3977
When 'Business Development / Sales' then 3707
When 'Business Intelligence' then 3708
When 'Business Objects' then 3846
When 'C' then 3978
When 'C#' then 3979
When 'C++' then 4083
When 'CakePHP' then 3980
When 'Call Centre' then 3709
When 'Cantonese' then 3981
When 'CAT5' then 3847
When 'CAT6' then 4084
When 'CCA' then 3848
When 'CCDP' then 3849
When 'CCEE' then 4085
When 'CCI' then 3850
When 'CCIA' then 3982
When 'CCIE' then 3983
When 'CCNA' then 3710
When 'CCNP' then 3984
When 'CCSP' then 3711
When 'CEO' then 3712
When 'Chaiwat' then 4086
When 'Change' then 4087
When 'Change Analyst' then 3851
When 'Change Manager' then 3713
When 'Chemical / Material' then 3096
When 'Chief Financial Officer' then 3852
When 'Chief Operations Officer' then 3714
When 'Chief Technical Officer' then 4088
When 'CIO' then 3715
When 'CIO / CTO' then 3985
When 'Cisco' then 3986
When 'Cisco (Gen)' then 4089
When 'CISM' then 3853
When 'CISSP' then 3987
When 'Citirx (Gen)' then 3716
When 'Citrix' then 4090
When 'Classic ASP' then 3717
When 'Clojure' then 3988
When 'Cloud' then 3718
When 'CMOS' then 3989
When 'CMS (Gen)' then 3719
When 'Cobol' then 4091
When 'Cognos' then 3854
When 'Coins' then 4092
When 'ColdFusion' then 3855
When 'Construction' then 3990
When 'Council' then 3991
When 'CRM' then 3992
When 'Crystal Reports' then 3720
When 'CS Core' then 3856
When 'CSS' then 3857
When 'CSS3' then 4093
When 'Cucumber' then 4094
When 'Data Science' then 3993
When 'Database Adminstrator' then 3858
When 'Database Developer' then 3994
When 'Database Specialist' then 3859
When 'Databases' then 4095
When 'DataCentre' then 3995
When 'Datastage' then 3721
When 'Datawarehouse' then 4096
When 'DB2' then 4097
When 'Delphi' then 4098
When 'Developer' then 3860
When 'Developer/Programmer' then 4099
When 'Development' then 3722
When 'Development Manager' then 3861
When 'DHTML' then 3723
When 'DHTMLX' then 3862
When 'Digital' then 3996
When 'Director' then 3997
When 'DirectX' then 3724
When 'Discoverer' then 4100
When 'Django' then 3725
When 'Document Controller' then 4101
When 'DOJO' then 4102
When 'Dplyr' then 4103
When 'Drupal' then 3726
When 'Dundee' then 3863
When 'Dynamics' then 3864
When 'Dynamics (Gen)' then 3727
When 'Dynamics Ax' then 3998
When 'Dynamics Crm' then 3728
When 'Dynamics Gp' then 3999
When 'Dynamics Nav' then 3729
When 'Dynamics Sl' then 3730
When 'Dynatrace' then 4000
When 'Eclipse' then 3865
When 'Edinburgh' then 3731
When 'efacs' then 4001
When 'EJB' then 3732
When 'Elastic Search' then 3866
When 'Embedded C' then 4104
When 'Embedded C++' then 3733
When 'Engineering' then 3097
When 'Epicor' then 3734
When 'Ericsson' then 3867
When 'ERP' then 4105
When 'ERP Consultant' then 3735
When 'Essbase' then 4002
When 'ETL' then 3736
When 'Excel' then 4003
When 'Exchange' then 3868
When 'Executive Management' then 3098
When 'EXTGWT' then 3737
When 'EXTJS' then 4004
When 'F#' then 3738
When 'Fedora' then 4106
When 'Fife' then 4107
When 'Financial Director' then 4108
When 'Firewall' then 4005
When 'FMCG' then 3869
When 'FMEA' then 4109
When 'Forms' then 4006
When 'Fortran' then 4007
When 'French' then 3870
When 'Front End Web' then 3739
When 'Functional Testing' then 4008
When 'Games Developer' then 3871
When 'German' then 4110
When 'GIS' then 4111
When 'Glasgow' then 3740
When 'Glovia' then 4009
When 'Gnu' then 3741
When 'Government' then 4010
When 'Group Policy' then 4112
When 'Groupwise' then 4011
When 'Hadoop' then 3872
When 'Handheld' then 3742
When 'HAZOP' then 4113
When 'Helpdesk Manager' then 3743
When 'Hibernate' then 4012
When 'HP-UX' then 4115
When 'HPUX' then 4114
When 'HR' then 4116
When 'HR, GA & Facilities' then 3099
When 'HSE' then 3744
When 'HTML' then 4013
When 'HTML5' then 4117
When 'Human Resource(sample only)' then 2981
When 'Hyperion' then 4118
When 'IBM' then 4014
When 'IFS' then 4119
When 'IIS' then 3745
When 'Industrial Equipment / Plant / Parts' then 3101
When 'Infor' then 4015
When 'Informatica' then 3873
When 'Infrastructure' then 3874
When 'Infrastructure Manager' then 3875
When 'Infrastructure Project Manager' then 3876
When 'Ingress' then 3746
When 'Interest In Big Data' then 3877
When 'iPhone' then 3878
When 'Ipython' then 4016
When 'ISO20071' then 3879
When 'IT & Telecoms' then 3100
When 'IT Director' then 3880
When 'IT Manager' then 4017
When 'Italian' then 3747
When 'Iterative' then 4120
When 'ITIL' then 3748
When 'J2EE' then 3881
When 'J2ME' then 3882
When 'J2SE' then 3749
When 'Jaspersoft' then 3750
When 'Java' then 3883
When 'JavaScript' then 3751
When 'JBoss' then 3884
When 'Jd Edwards E1' then 4018
When 'JD Edwards World' then 4019
When 'JDBC' then 3752
When 'JDeveloper' then 4121
When 'Jidapa' then 3753
When 'Joomla' then 3754
When 'jQuery' then 3885
When 'JSF' then 4122
When 'JSP' then 4020
When 'JSPHP' then 3886
When 'JUnit' then 4123
When 'JWT' then 3755
When 'Jython' then 4124
When 'Kernel' then 3756
When 'Kimball' then 3757
When 'Knitr' then 3887
When 'Korn Shell' then 3758
When 'Lamp' then 3759
When 'LAN' then 4125
When 'Languages' then 4021
When 'Lean/6 Sigma' then 4126
When 'Legal' then 3102
When 'LINQ' then 4022
When 'Linux' then 3888
When 'Load testing' then 3889
When 'LoadRunner' then 3760
When 'Logistics' then 3890
When 'Lotus' then 3891
When 'Lotus Domino' then 3892
When 'LTE' then 4023
When 'Lync' then 3893
When 'Magento' then 3894
When 'Mahout' then 4024
When 'MAN' then 3895
When 'Manager' then 3761
When 'Managing Director' then 3896
When 'Mandarin' then 3897
When 'Manual Testing' then 3762
When 'Manufacturing' then 3898
When 'Mapics' then 3763
When 'Masters  Level' then 3899
When 'Matlab' then 3764
When 'Maven' then 4025
When 'Maximo' then 3900
When 'mba' then 3765
When 'MCDBA' then 4026
When 'Mcdoogs' then 3901
When 'MCITP' then 3902
When 'MCP' then 3903
When 'MCPD' then 3904
When 'MCSE' then 3905
When 'MCTP' then 3906
When 'MCTS' then 4027
When 'Medical Device / Diagnostics / Analytical' then 3103
When 'Mercury' then 3766
When 'Microsoft' then 4028
When 'Microsoft & SQL Server' then 3907
When 'Microsoft BI' then 4127
When 'Microsoft Partner' then 3908
When 'Microstrategy' then 3909
When 'Microwave' then 3910
When 'Mobile' then 3911
When 'Mobile Development' then 4128
When 'Monitoring' then 4129
When 'MooTools' then 4130
When 'Movex' then 4131
When 'MRP' then 4029
When 'Ms Bi Developer' then 4030
When 'Mura' then 3912
When 'Music' then 2982
When 'MVC' then 4132
When 'MySQL' then 4031
When 'NetAct' then 4133
When 'NetBeans' then 3913
When 'Netezza' then 3767
When 'NetScaler' then 4134
When 'Netsuite' then 3914
When 'Netware' then 3768
When 'Network Engineer' then 4032
When 'Network Manager' then 4033
When 'Networking' then 4135
When 'nHibernate' then 4034
When 'NHS' then 3769
When 'Nosql' then 3770
When 'Novell (Gen)' then 3771
When 'NSN' then 4136
When 'OBIEE' then 3915
When 'Ods' then 4035
When 'Oil And Gas' then 4137
When 'Olap' then 4138
When 'Oltp' then 3916
When 'OO' then 4139
When 'OpenSUSE' then 4036
When 'Operations Manager' then 4037
When 'Oracle' then 4038
When 'Oracle 10g' then 4140
When 'Oracle 11g' then 4141
When 'Oracle 11gR2' then 4142
When 'Oracle 8i' then 3772
When 'Oracle 9i' then 3773
When 'Oracle Application Server' then 3774
When 'Oracle BI' then 3917
When 'Oracle Collaboration' then 3918
When 'Oracle Crm' then 3919
When 'Oracle DataGuard' then 3920
When 'Oracle Designer' then 3775
When 'Oracle Dev (Gen)' then 4143
When 'Oracle ERP' then 3921
When 'Oracle Flashback' then 4144
When 'Oracle Lite' then 3922
When 'Oracle OCA' then 4145
When 'Oracle OCP' then 3923
When 'Oracle OEM' then 4039
When 'Oracle RAC' then 3924
When 'Oracle RMAN' then 3925
When 'OSS' then 4040
When 'Outlook' then 3926
When 'OWB' then 3927
When 'Panda' then 4041
When 'Panorama' then 4146
When 'Pascal' then 3776
When 'Pegasus' then 3777
When 'Pentaho' then 3778
When 'PeopleSoft' then 3928
When 'Performance Point' then 3929
When 'Performance Testing' then 4147
When 'Perl' then 4148
When 'Perth' then 4149
When 'Pharawi' then 3930
When 'Pharma' then 4150
When 'Pharmaceutical / CRO / Reagents / Personal Care' then 3104
When 'PHD Level' then 3931
When 'PHP' then 3779
When 'PHP (General)' then 4151
When 'PHP5' then 3780
When 'PL/SQL' then 3932
When 'PMO' then 4152
When 'Prairie' then 4153
When 'Process Safety' then 3781
When 'Procurement' then 3933
When 'Product Manager' then 4042
When 'Prognoz' then 3782
When 'Programme Manager' then 4154
When 'Progress' then 3934
When 'Project Manager' then 3783
When 'PS Core' then 4155
When 'Publisher' then 3935
When 'Python' then 3784
When 'QA' then 3785
When 'Qlikview' then 4156
When 'QTP' then 3936
When 'RAD' then 3786
When 'Radio' then 3787
When 'RCN' then 3937
When 'Real Estate & Insurance' then 3105
When 'RedHat' then 3788
When 'Regression Testing' then 4043
When 'Reports' then 3938
When 'Retail' then 4157
When 'RF' then 3939
When 'Ruby' then 3789
When 'Ruby on Rails' then 4158
When 'RUP' then 3790
When 'Russian' then 3940
When 'Sage' then 3791
When 'Sales & Marketing' then 3106
When 'Salesforce' then 4159
When 'Salient' then 4044
When 'Samba' then 4160
When 'SAP' then 3792
When 'SAP ABAP' then 3793
When 'SAP BI' then 4161
When 'Sap Crm' then 4045
When 'SAP CRM (Gen)' then 4046
When 'SAP CRM Interaction Centre' then 4162
When 'SAP CRM Marketing' then 3941
When 'SAP CRM Web' then 3794
When 'SAP FICO' then 3795
When 'SAP Financials' then 4163
When 'SAP HR' then 4047
When 'SAP MM' then 3942
When 'SAP PP' then 3943
When 'SAP QM' then 3944
When 'SAS' then 3796
When 'SBS' then 4164
When 'Scala' then 4165
When 'SCCM' then 3945
When 'Script.aculo.us' then 4048
When 'Scrum' then 3946
When 'Security Testing' then 4049
When 'Selenium' then 4050
When 'Semiconductor / Embedded Device' then 3107
When 'Server Engineer' then 4051
When 'Server Support' then 3947
When 'Service Delivery Manager' then 4166
When 'Servlets' then 3948
When 'Sharepoint' then 3797
When 'Shell Script' then 3949
When 'Siebel' then 4167
When 'Siebel BI' then 4168
When 'Silverlight' then 4169
When 'SiteCore' then 3798
When 'Sittikarn' then 4052
When 'Software' then 3950
When 'Software Developer' then 3951
When 'Software Project Manager' then 3952
When 'Solaris' then 3799
When 'Spanish' then 3953
When 'Spring' then 4170
When 'SQL' then 4171
When 'SQL Server' then 3800
When 'SQL Server 2000' then 3954
When 'SQL Server 2005' then 3801
When 'SQL Server 2008' then 4053
When 'SQL Server 2012' then 4054
When 'SQL Server 2014' then 4055
When 'SQL Server Compact' then 4172
When 'SSAS' then 4173
When 'SSIS' then 3802
When 'SSRS' then 4056
When 'Stirling' then 4174
When 'Storage' then 4057
When 'Struts' then 4058
When 'Sugar' then 4175
When 'Sun Accounts' then 3955
When 'Sun Certified' then 3803
When 'Supply Chain Management' then 3108
When 'Swing' then 3804
When 'Sybase' then 3805
When 'Symfony' then 3806
When 'Syspro' then 4059
When 'Systems Analyst' then 3807
When 'Systems Testing' then 4176
When 'T-SQL' then 4181
When 'Tableau' then 4060
When 'Tapestry' then 3956
When 'Targit' then 4061
When 'TCP/IP' then 3957
When 'Team Leader' then 3808
When 'Technical Consultancy' then 3809
When 'Test Director' then 4062
When 'Test Manager' then 4177
When 'Tester' then 4178
When 'Testing' then 3810
When 'TETRA Telecoms' then 3811
When 'TFS' then 4179
When 'Third Sector' then 3812
When 'Tibco' then 4180
When 'Tomcat' then 3958
When 'Traffic' then 4063
When 'Trainer' then 4064
When 'UAT' then 4182
When 'UI' then 3959
When 'UMTS' then 4065
When 'Unit Testing' then 4183
When 'Unix (Gen)' then 3960
When 'Unix Dev (Gen)' then 4184
When 'UNIX/LINUX' then 4185
When 'Utilities' then 4186
When 'UTRAN' then 3813
When 'UX' then 4066
When 'VB.Net' then 4067
When 'VBA' then 3961
When 'Verilog' then 3962
When 'Virtualisation' then 3814
When 'Visual Basic' then 3963
When 'VMODEL' then 4187
When 'Voice' then 3815
When 'VPN' then 3964
When 'W3C' then 3816
When 'Wamp' then 3817
When 'WAN' then 4068
When 'Waterfall' then 3965
When 'WCF' then 4188
When 'Web Designer' then 3818
When 'WF' then 3819
When 'WIFI' then 3966
When 'WiMAX' then 3967
When 'Windows Desktop' then 4189
When 'Windows Server' then 3820
When 'WLAN' then 3821
When 'WordPress' then 3968
When 'WPF' then 3822
When 'Xamarin' then 3823
When 'XAML' then 3969
When 'XenApp' then 4190
When 'Xendesktop' then 3824
When 'XenServer' then 4069
When 'XHTML' then 4070
When 'XML' then 3825
When 'XPATH' then 3826
When 'XQuery' then 4071
When 'XSL' then 3970
When 'XSLT' then 4072
When 'Yellowfin' then 4191
When 'YUI' then 3971
When 'Zend' then 3972
else '' end fe
from t
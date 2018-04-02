
with
-- SkillName: split by separate rows by comma, then combine them into SkillName
  SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '')
, sfe as (
                select
                 C.candidateID, concat(C.FirstName,' ',C.LastName) as fullname
                 , 2983 as 'fe'
                --, SN.SkillName
                , case sn.SkillName
when '1C' then 184
when 'Absence' then 185
when 'ActiveX' then 186
when 'Actuate' then 187
when 'Administer Training' then 188
when 'Administer Workforce' then 189
when 'ADP' then 190
when 'ADSL' then 191
when 'Advanced Configurator' then 192
when 'Advanced Planning' then 193
when 'Afrikaans' then 194
when 'Agile' then 195
when 'AIX' then 196
when 'ALE' then 197
when 'AME' then 198
when 'AMX BPM' then 199
when 'Android' then 200
when 'angular js' then 201
when 'Apache' then 202
when 'APO' then 203
when 'Application developer' then 204
when 'Application engine' then 205
when 'Application server' then 206
when 'AQ' then 207
when 'AquaLogic Architect' then 208
when 'AquaLogic BPM' then 209
when 'AquaLogic ESB' then 210
when 'Arbor' then 211
when 'Architect' then 212
when 'AS/400' then 213
when 'ASCP' then 214
when 'AX4' then 215
when 'Axure' then 216
when 'B2B Sales/Recruitment Consultant' then 217
when 'Back-End Developer' then 218
when 'BAM' then 219
when 'BAPI' then 220
when 'BC' then 221
when 'Benefits' then 222
when 'BI' then 223
when 'BI Architect' then 224
when 'bi publisher' then 225
when 'Big Data' then 226
when 'Big Data Architect' then 227
when 'BIG DATA Scientist' then 228
when 'BI-IP' then 229
when 'Billing' then 230
when 'BizTalk 2009' then 231
when 'Biztalk 2010' then 232
when 'BizTalk Systems Administrator' then 233
when 'BO' then 234
when 'BODI' then 235
when 'BOM' then 236
when 'BPM' then 237
when 'Bscs' then 238
when 'BSS Consultant' then 239
when 'BSS Solution Architect' then 240
when 'Bulgarian' then 241
when 'Business Analyst' then 242
when 'Business Intelligence' then 243
when 'Business Intelligence Architect' then 244
when 'Business Intelligence Business Analyst' then 245
when 'Business Intelligence Project Manager' then 246
when 'Business Process Consultant' then 247
when 'Business Rules' then 248
when 'BusinessEvents' then 249
when 'BusinessWorks 2nd' then 250
when 'BW' then 251
when 'C' then 252
when 'C#' then 253
when 'Ca' then 254
when 'Candidate Gateway' then 255
when 'Cantonese' then 256
when 'Captiva' then 257
when 'Cassandra' then 258
when 'CDMA/WCDMA' then 259
when 'Cim' then 260
when 'Cisco' then 261
when 'Clojure' then 262
when 'cloud' then 263
when 'Cloud Architect' then 264
when 'Cloud Development' then 265
when 'Cloud Security' then 266
when 'cloudera' then 267
when 'CM' then 268
when 'CMS' then 269
when 'Coach Agile' then 270
when 'CODA' then 271
when 'Cognos TM1' then 272
when 'Collaborator' then 273
when 'Configuration Manager' then 274
when 'Configurator' then 275
when 'Consulting' then 276
when 'CONTROL M' then 277
when 'Core Networks' then 278
when 'Cornerstone' then 279
when 'CouchDB' then 280
when 'CRM' then 281
when 'Croatian' then 282
when 'CrossWorlds' then 283
when 'CTO' then 284
when 'Czech' then 285
when 'd3js' then 286
when 'DADS-U' then 287
when 'Danish' then 288
when 'Data Analysis' then 289
when 'Data Cleansing' then 290
when 'DATA INTEGRATION' then 291
when 'Data Migration' then 292
when 'Data Quality' then 293
when 'data science' then 294
when 'Data Warehousing' then 295
when 'Database Architect' then 296
when 'DATAMART' then 297
when 'DataStage' then 298
when 'Dell Quest' then 299
when 'DevOps' then 300
when 'Digital PM' then 301
when 'Director of Marketing and Communications' then 302
when 'django' then 303
when 'Documentum' then 304
when 'DOS' then 305
when 'Drupal' then 306
when 'DS' then 307
when 'Dynamics Functional' then 308
when 'EAM' then 309
when 'EBTAX' then 310
when 'ECONFIGURATOR' then 311
when 'eDevelopment' then 312
when 'EIM' then 313
when 'Electronic Money' then 314
when 'eMarketing' then 315
when 'EmberJs' then 316
when 'Ems' then 317
when 'Engineer' then 318
when 'English' then 319
when 'eRecruitment' then 320
when 'ESA' then 321
when 'eSales' then 322
when 'eSettlement' then 323
when 'ESSBase' then 324
when 'Est' then 325
when 'Ethernet' then 326
when 'ETL' then 327
when 'EX' then 328
when 'EZpublish' then 329
when 'FA' then 330
when 'FAH' then 331
when 'Fi-co' then 332
when 'Filenet' then 333
when 'Finance AX Module' then 334
when 'Finance NAV Module' then 335
when 'Firewall' then 336
when 'Flash' then 337
when 'Flume' then 338
when 'FM' then 339
when 'Foodtech' then 340
when 'Forms' then 341
when 'FS-CD' then 342
when 'FSCM' then 343
when 'FS-CM' then 344
when 'FS-PM' then 345
when 'Full Stack' then 346
when 'Functional AX' then 347
when 'Functional NAV' then 348
when 'Fusion HR Consultant' then 349
when 'GC' then 350
when 'Genesys' then 351
when 'GI' then 352
when 'GIT' then 353
when 'Global Payroll' then 354
when 'Golang' then 355
when 'Gprs' then 356
when 'GSM' then 357
when 'Hal' then 358
when 'Hbase' then 359
when 'Hibernate' then 360
when 'Hive' then 361
when 'hortonworks' then 362
when 'HP Cloud' then 363
when 'HPQC' then 364
when 'HTML' then 365
when 'HYBRID' then 366
when 'Hybride Cloud' then 367
when 'IAM' then 368
when 'IBM BPM' then 369
when 'IBM Identity Manager' then 370
when 'IBM MDM' then 371
when 'IDAM' then 372
when 'IHM' then 373
when 'iLearning' then 374
when 'Interfacing' then 375
when 'Interim Management' then 376
when 'INV' then 377
when 'IOT' then 378
when 'IRANIAN' then 379
when 'IS Pharma' then 380
when 'ISAM' then 381
when 'ISAMESSO' then 382
when 'ISDS' then 383
when 'IS-H' then 384
when 'IS-Media' then 385
when 'IS-RE' then 386
when 'IS-U' then 387
when 'Italian' then 388
when 'ITIL' then 389
when 'Java Architect' then 390
when 'Java Developer' then 391
when 'JAVA FX' then 392
when 'Java lead dev' then 393
when 'Javascript' then 394
when 'JD EDWARDS' then 395
when 'JDEDWARDS' then 396
when 'Jira' then 397
when 'Joomla!' then 398
when 'JSON' then 399
when 'JSP' then 400
when 'Kafka' then 401
when 'KAZAKH' then 402
when 'Kshell' then 403
when 'L4G' then 404
when 'Laravel' then 405
when 'LE' then 406
when 'Lean' then 407
when 'Lease Management' then 408
when 'Lithuanian' then 409
when 'Loyalty' then 410
when 'LSO' then 411
when 'LTE' then 412
when 'Machine Learning' then 413
when 'Manufacturing NAV Module' then 414
when 'Marketing CRM Module' then 415
when 'Matlab' then 416
when 'Maven' then 417
when 'MDX' then 418
when 'MEAN Stack' then 419
when 'Mercury' then 420
when 'Message Broker' then 421
when 'Meteor JS' then 422
when 'Microsoft CRM' then 423
when 'Microsoft CRM Developer' then 424
when 'Microsoft CRM Director' then 425
when 'Microsoft Dynamics AX' then 426
when 'Microsoft Dynamics AX Finance' then 427
when 'Microsoft Dynamics AX PM' then 428
when 'Microsoft Dynamics AX Technical Consultant' then 429
when 'Microsoft Dynamics CRM' then 430
when 'Microsoft Dynamics CRM PM' then 431
when 'microsoft dynamics nav' then 432
when 'Microsoft Dynamics NAV BA' then 433
when 'Microsoft Dynamics NAV Developer' then 434
when 'Microsoft Dynamics NAV Director' then 435
when 'Microsoft Dynamics NAV pre-sales' then 436
when 'MongoDB' then 437
when 'MRP' then 438
when 'MSCA' then 439
when 'MySQL' then 440
when 'NAS' then 441
when 'Netbackup' then 442
when 'Netweaver Gateway' then 443
when 'Network administrator' then 444
when 'Network engineer' then 445
when 'NORD' then 446
when 'norweigan' then 447
when 'NoSQL' then 448
when 'OAM' then 449
when 'ODI' then 450
when 'ODS' then 451
when 'OIL HPM' then 452
when 'OIL TSW' then 453
when 'OKS' then 454
when 'OLM' then 455
when 'OM' then 456
when 'ONDEMAND' then 457
when 'OPENTEXT' then 458
when 'OPM' then 459
when 'Oracle' then 460
when 'Oracle Administrator' then 461
when 'Oracle Database Developer' then 462
when 'Oracle EBS' then 463
when 'Oracle EBS CRM Functional Consultant' then 464
when 'Oracle EBS CRM Project Director' then 465
when 'Oracle EBS CRM Project Manager' then 466
when 'Oracle EBS Financials Functional Consultant' then 467
when 'Oracle EBS Financials Project Director' then 468
when 'Oracle EBS Financials Support Consultant' then 469
when 'Oracle EBS HRMS Business Analyst' then 470
when 'Oracle EBS HRMS Project Manager' then 471
when 'Oracle EBS HRMS Support Consultant' then 472
when 'Oracle EBS HRMS Technical Analyst' then 473
when 'Oracle EBS HRMS Techno Functional' then 474
when 'Oracle EBS Logistics Business Analyst' then 475
when 'Oracle EBS Logistics Functional Consultant' then 476
when 'Oracle EBS Logistics Project Director' then 477
when 'Oracle EBS Logistics Techno Functional' then 478
when 'Oracle Fusion Financials' then 479
when 'Oracle Fusion Middleware' then 480
when 'Oracle Fusion Procurement' then 481
when 'Oracle Identity Management' then 482
when 'Oracle SOA' then 483
when 'Oracle SOA Architect' then 484
when 'Oracle SOA Tester' then 485
when 'OST' then 486
when 'OTA' then 487
when 'OTC' then 488
when 'OTL' then 489
when 'OTM' then 490
when 'Ouest' then 491
when 'PA' then 492
when 'PAB' then 493
when 'PAC' then 494
when 'Pascal' then 495
when 'PC' then 496
when 'Pega CLSA' then 497
when 'Pega CSA' then 498
when 'PEGA Developer' then 499
when 'Pentaho' then 500
when 'Peoplesoft Architect' then 501
when 'PeopleSoft Campus Developer' then 502
when 'PeopleSoft Campus Functional' then 503
when 'PeopleSoft Campus Project Manager' then 504
when 'PeopleSoft CRM Functional' then 505
when 'PeopleSoft CRM Project Manager' then 506
when 'PeopleSoft CRM Techno Functional' then 507
when 'PeopleSoft EPM Project Manager' then 508
when 'PeopleSoft EPM Techno Functional' then 509
when 'PeopleSoft ESA Functional' then 510
when 'PeopleSoft ESA Techno Functional' then 511
when 'PeopleSoft Financials Developer' then 512
when 'PeopleSoft Financials Functional' then 513
when 'PeopleSoft Financials Techno Functional' then 514
when 'PeopleSoft FSCM Developer' then 515
when 'PeopleSoft FSCM Functional' then 516
when 'PeopleSoft FSCM Project Manager' then 517
when 'PeopleSoft HR Developer' then 518
when 'PeopleSoft HR Project Manager' then 519
when 'PeopleSoft HR Techno Functional' then 520
when 'PeopleSoft Payroll Functional' then 521
when 'PeopleSoft Payroll Project Manager' then 522
when 'PeopleSoft Payroll Techno Functional' then 523
when 'Peoplesoft Tester' then 524
when 'PeopleTools' then 525
when 'PHP' then 526
when 'PHP lead dev' then 527
when 'PI' then 528
when 'Play Framework' then 529
when 'PM/Program Manager NAV' then 530
when 'Policy Manager' then 531
when 'Polish' then 532
when 'Portal' then 533
when 'Portuguese' then 534
when 'Postgresql' then 535
when 'postsales' then 536
when 'PowerShell' then 537
when 'Pre-Production Engineer' then 538
when 'Presales' then 539
when 'Prestashop' then 540
when 'Pretashop' then 541
when 'Pricing' then 542
when 'ProcessServer' then 543
when 'Procurement AX Module' then 544
when 'PRODUCT CATALOGUE' then 545
when 'Product Director' then 546
when 'Product Manager' then 547
when 'Production Analyst' then 548
when 'Project Manager' then 549
when 'Purchasing AX Module' then 550
when 'Python' then 551
when 'QM' then 552
when 'QP' then 553
when 'Quality Management' then 554
when 'QualityCenter' then 555
when 'quant' then 556
when 'R&D' then 557
when 'RAN' then 558
when 'Recruitment' then 559
when 'Recruitment Resourcer' then 560
when 'Redhat' then 561
when 'Requirement Engineering' then 562
when 'Responsive Design' then 563
when 'Retail AX Module' then 564
when 'Risk Backtesting' then 565
when 'Risk Credit' then 566
when 'Risk LGD' then 567
when 'Risk Management' then 568
when 'Risk market quant' then 569
when 'Risk market regulatory' then 570
when 'Risk MOA' then 571
when 'Risk PD' then 572
when 'Risk Score Carding' then 573
when 'Risk Stress Testing' then 574
when 'RMCA' then 575
when 'Romanian' then 576
when 'RPG' then 577
when 'Ruby' then 578
when 'SA' then 579
when 'SAGE' then 580
when 'Sage 1000' then 581
when 'Sage Paie' then 582
when 'Sage x3' then 583
when 'Sales and Marketing NAV Module' then 584
when 'Sales AX Module' then 585
when 'Salesforce.com Administrator' then 586
when 'Salesforce.com Functional' then 587
when 'Salesforce.com Project Manager' then 588
when 'SAP' then 589
when 'SAP ABAP Developer' then 590
when 'SAP Admin' then 591
when 'SAP AMOA Finance Consultant' then 592
when 'SAP AMOA Logistic Consultant' then 593
when 'SAP APO CONSULTANT' then 594
when 'SAP Bank Analyzer Consultant' then 595
when 'SAP Bank Analyzer Techno/Fun Consultant' then 596
when 'SAP BCA Consultant' then 597
when 'SAP BCS Consultant' then 598
when 'SAP BCS Techno/Fun Consultant' then 599
when 'SAP BFC Consultant' then 600
when 'SAP BI Consultant' then 601
when 'SAP BO Administrator' then 602
when 'SAP BPC Consultant' then 603
when 'SAP BRF' then 604
when 'SAP BRIM Consultant' then 605
when 'SAP BW Consultant' then 606
when 'SAP BW Techno/Fun Consultant' then 607
when 'SAP CFM Consultant' then 608
when 'SAP CFM Techno/Fun Consultant' then 609
when 'SAP CML Consultant' then 610
when 'SAP CML Techno/Fun Consultant' then 611
when 'SAP CO Consultant' then 612
when 'SAP CO Techno/Fun Consultant' then 613
when 'SAP Core Banking Consultant' then 614
when 'SAP Core Banking Techno/Fun Consultant' then 615
when 'SAP CRM Developer' then 616
when 'SAP CS Consultant' then 617
when 'SAP CS Techno/Fun Consultant' then 618
when 'SAP DATA MIGRATION CONSULTANT' then 619
when 'SAP EDI Techno/Fun Consultant' then 620
when 'SAP EM Consultant' then 621
when 'SAP EWM Consultant' then 622
when 'Sap Fi Consultant' then 623
when 'SAP FI Techno/Fun Consultant' then 624
when 'SAP FI-AA Techno/Functional Consultant' then 625
when 'SAP Financials Business Analyst' then 626
when 'SAP FM Techno/Fun Consultant' then 627
when 'SAP FS-CD Consultant' then 628
when 'SAP FS-CS Consultant' then 629
when 'SAP FS-PM Consultant' then 630
when 'SAP FS-RI Consultant' then 631
when 'SAP HCM Techno/Fun Consultant' then 632
when 'SAP HR' then 633
when 'SAP IS-A&D Consultant' then 634
when 'SAP IS-AFS Consultant' then 635
when 'SAP IS-AFS Techno/Fun Consultant' then 636
when 'SAP IS-Auto Consultant' then 637
when 'SAP IS-Auto Techno/Fun Consultant' then 638
when 'SAP IS-EH&S Techno/Fun Consultant' then 639
when 'SAP IS-Media Techno/Fun Consultant' then 640
when 'SAP IS-Mill Consultant' then 641
when 'SAP IS-RE Consultant' then 642
when 'SAP IS-Retail Consultant' then 643
when 'SAP IS-Retail Techno/Fun Consultant' then 644
when 'SAP IS-T Techno/Fun Consultant' then 645
when 'SAP IS-U CONSULTANT' then 646
when 'SAP JVA Consultant' then 647
when 'SAP LE Consultant' then 648
when 'SAP LE Techno/Fun Consultant' then 649
when 'SAP MM Techno/Fun Consultant' then 650
when 'SAP Netweaver Architect /Consultant' then 651
when 'SAP PM Techno/Fun Consultant' then 652
when 'SAP PMO' then 653
when 'SAP PP Techno/Fun Consultant' then 654
when 'SAP PP-PI' then 655
when 'SAP PS Consultant' then 656
when 'SAP PS-CD Consultant' then 657
when 'SAP PS-CD Techno/Functional Consultant' then 658
when 'SAP QM Consultant' then 659
when 'SAP QM Techno/Fun Consultant' then 660
when 'SAP RE Consultant' then 661
when 'SAP Recruitment Consultant' then 662
when 'SAP REFX' then 663
when 'SAP RM Consultant' then 664
when 'SAP SCM Business Analyst' then 665
when 'SAP SNP Consultant' then 666
when 'SAP SRM Consultant' then 667
when 'SAP TM Consultant' then 668
when 'Sap Trainer' then 669
when 'SAP VARIANT CONFIGURATOR' then 670
when 'SAP VIM Consultant' then 671
when 'SAP VIM Techno/Fun Consultant' then 672
when 'SAP Workflow Developer' then 673
when 'SAS' then 674
when 'SDH/PDH' then 675
when 'SDLC' then 676
when 'SEC Business Continuity' then 677
when 'Sec Cert CCNA' then 678
when 'SEC cert CISM' then 679
when 'SEC cert CISP' then 680
when 'SEC cert CISSP' then 681
when 'SEC cert ISO 27001' then 682
when 'SEC CISO' then 683
when 'SEC Cryptography' then 684
when 'SEC Cyber Architect' then 685
when 'SEC Cyber Manager' then 686
when 'SEC Data Protection' then 687
when 'SEC Disaster Recovery' then 688
when 'SEC Encryption' then 689
when 'SEC FIREWALL' then 690
when 'SEC GDPR' then 691
when 'SEC IAM Technical' then 692
when 'SEC Infrastructure' then 693
when 'SEC IOT-Security' then 694
when 'SEC IT AUDIT' then 695
when 'SEC IT Auditor' then 696
when 'SEC Juniper' then 697
when 'SEC Network Security' then 698
when 'SEC Pre-Sales Security' then 699
when 'SEC Sales Security' then 700
when 'SEC Security Risk and Governance' then 701
when 'SEC SIEM Vulnerability Asst' then 702
when 'SEC SOC Security Analyst' then 703
when 'SEC TCP/IP' then 704
when 'SEC Wireless Security' then 705
when 'Security' then 706
when 'Security Cleared - UK Current' then 707
when 'Security Cleared - UK Potential' then 708
when 'Security Engineer' then 709
when 'Seeburger' then 710
when 'Selenium' then 711
when 'Self-Service' then 712
when 'Selligent Functional' then 713
when 'SENTINEL' then 714
when 'SEO Consultant' then 715
when 'Servicenow' then 716
when 'Sharepoint' then 717
when 'Siebel AMOE' then 718
when 'Siebel Architect' then 719
when 'Siebel Project Manager' then 720
when 'Siebel Trainer' then 721
when 'SIEBELTOOLS' then 722
when 'Silex' then 723
when 'Sitecore' then 724
when 'SLA' then 725
when 'SLOVAKIAN' then 726
when 'SMTP' then 727
when 'SOA' then 728
when 'SOA / BPM' then 729
when 'SOA Architect' then 730
when 'Solaris' then 731
when 'Solution Architect' then 732
when 'SPIP' then 733
when 'Splunk' then 734
when 'SQL Server' then 735
when 'Sql Server Administrator' then 736
when 'Sqoop' then 737
when 'SQR' then 738
when 'SSIS' then 739
when 'SSO' then 740
when 'SSRS' then 741
when 'Startup' then 742
when 'STLC' then 743
when 'Stockage' then 744
when 'Successfactor Consultant' then 745
when 'Successfactors Functional Consultant' then 746
when 'Successfactors PM/GM' then 747
when 'SuccessFactors Recruitment' then 748
when 'Successfactors Recruting' then 749
when 'SuccessFactors Variable Pay' then 750
when 'SuccessFactors Workforce Analytics' then 751
when 'Sud Est' then 752
when 'Sud Ouest' then 753
when 'Sun' then 754
when 'Supply Chain' then 755
when 'Supply Chain AX Module' then 756
when 'Support' then 757
when 'Swedish' then 758
when 'SWING' then 759
when 'Sybase' then 760
when 'Symfony' then 761
when 'System Administrator Linux' then 762
when 'System Administrator Unix' then 763
when 'System Engineer IBM' then 764
when 'System Engineer Unix' then 765
when 'System Engineer Windows' then 766
when 'T&L' then 767
when 'Tableau' then 768
when 'Talentsoft Consultant' then 769
when 'TAM' then 770
when 'Technical Project Manager' then 771
when 'TELCO' then 772
when 'Telco Design' then 773
when 'Telco Installation Integration' then 774
when 'Telco Project Manager' then 775
when 'Telco Testing' then 776
when 'Telecom Project Manager' then 777
when 'Test Analyst' then 778
when 'Test Engineer' then 779
when 'Thai' then 780
when 'TIBCO Developer' then 781
when 'Tibco Project Manager' then 782
when 'Tibco Systems Administrator' then 783
when 'Tivoli' then 784
when 'TOGAF' then 785
when 'Trainer' then 786
when 'Transition Manager' then 787
when 'Transmission' then 788
when 'Turkish' then 789
when 'TV' then 790
when 'Typo3' then 791
when 'UCM' then 792
when 'UI designer' then 793
when 'UI/User Interface' then 794
when 'Ukrainian' then 795
when 'Umts' then 796
when 'Update Software Technical' then 797
when 'VBS' then 798
when 'VirtualBox' then 799
when 'Virtualization' then 800
when 'VNC' then 801
when 'VoIP' then 802
when 'Vsphere' then 803
when 'WAN' then 804
when 'Web ADI' then 805
when 'Web architect' then 806
when 'Web Design' then 807
when 'Web Development' then 808
when 'webdynpro' then 809
when 'Webfocus' then 810
when 'WebMethods' then 811
when 'Webmethods architect' then 812
when 'Webmethods Project Manager' then 813
when 'WebMethods Systems Administrator' then 814
when 'Websphere' then 815
when 'Websphere business Integrator' then 816
when 'WebSphere Business Integrator Message Broker' then 817
when 'Websphere Developer' then 818
when 'WebSphere Systems Administrator' then 819
when 'WebSphere Tester' then 820
when 'Windows NT' then 821
when 'Windows Server' then 822
when 'WM' then 823
when 'WMQI' then 824
when 'Workday Advanced Compensation' then 825
when 'Workday Bonus' then 826
when 'Workday Consultant' then 827
when 'Workday Core Connector' then 828
when 'Workday Data Conversion' then 829
when 'Workday Engagement Manager' then 830
when 'Workday Finance Functional Consultant' then 831
when 'Workday HCM Functional Consultant' then 832
when 'Workday HCM Technical Consultant' then 833
when 'Workday Integration Core' then 834
when 'Workday Integrations Certified' then 835
when 'Workday Learning' then 836
when 'Workday Payroll' then 837
when 'Workday Planning' then 838
when 'Workday Pre-sales' then 839
when 'Workday Reporting' then 840
when 'Workday Security' then 841
when 'Workday Studio' then 842
when 'Workday Talent & Performance' then 843
when 'Workday Time Tracking' then 844
when 'WORKFORCE ADMINISTRATION' then 845
when 'WPF' then 846
when 'WPM' then 847
when 'WSO2' then 848
when 'Xamarin' then 849
when 'Xcelsius' then 850
when 'XHTML' then 851
when 'XML' then 852
when 'XML Publisher' then 853
when 'XSLT' then 854
when 'Zookeeper' then 855
when '$Univers' then 856
when '.NET' then 857
when '4G' then 858
when 'AA' then 859
when 'ABAP' then 860
when 'Abroad' then 861
when 'Accountant' then 862
when 'Acd' then 863
when 'ActionScript' then 864
when 'Active Directory' then 865
when 'Active Enterprise' then 866
when 'Active Matrix' then 867
when 'Actuary' then 868
when 'Adapter SDK' then 869
when 'ADI' then 870
when 'Administrator' then 871
when 'Adobe' then 872
when 'Adonix' then 873
when 'Adoption' then 874
when 'Advanced Collections' then 875
when 'AFF' then 876
when 'AFS' then 877
when 'Ajax' then 878
when 'Alfresco' then 879
when 'AM' then 880
when 'Amazon Web Services' then 881
when 'AMF' then 882
when 'AMOA' then 883
when 'Analytics' then 884
when 'AP' then 885
when 'API' then 886
when 'Application designer' then 887
when 'Application Messaging' then 888
when 'Application Packages' then 889
when 'Approval Management' then 890
when 'APS' then 891
when 'Aptitude' then 892
when 'Aqualogic' then 893
when 'AR' then 894
when 'Arabic' then 895
when 'ARIBA' then 896
when 'Art Director' then 897
when 'AS3' then 898
when 'ASP.NET' then 899
when 'ATM' then 900
when 'AX' then 901
when 'AX2009' then 902
when 'AX2012' then 903
when 'Axapta' then 904
when 'Azure AAD' then 905
when 'BACKBONE' then 906
when 'Bale' then 907
when 'Bank Reconciliation' then 908
when 'Banking Consultant' then 909
when 'Bash' then 910
when 'Basis' then 911
when 'BCS' then 912
when 'BIA' then 913
when 'Big Data Analyst' then 914
when 'Big Data Chief Officer' then 915
when 'Big Data Developer' then 916
when 'BIG DATA Ecole Gr.1' then 917
when 'Big Data Engineer' then 918
when 'Big Data Integration Specialist' then 919
when 'Big Data Miner' then 920
when 'Big Data Visualizer' then 921
when 'BizTalk Architect' then 922
when 'BizTalk Business Analyst' then 923
when 'Biztalk Developer' then 924
when 'BizTalk Functional Consultant' then 925
when 'Bootstrap' then 926
when 'BPR' then 927
when 'BPS' then 928
when 'Brio' then 929
when 'BRM' then 930
when 'Business Intelligence Developer' then 931
when 'Business Intelligence Functional Analyst' then 932
when 'Business Intelligence Project Director' then 933
when 'Business Intelligence Techno Functional Consultant' then 934
when 'Business Intelligence Trainer' then 935
when 'Business Transformation' then 936
when 'BusinessConnect' then 937
when 'BusinessStudio' then 938
when 'BusinessWorks' then 939
when 'BusinessWorks 6' then 940
when 'C++' then 941
when 'Campus Solutions' then 942
when 'Capital Market Consultant' then 943
when 'CC' then 944
when 'CCNA/CCNP' then 945
when 'Centre' then 946
when 'Change management' then 947
when 'Charging Systems' then 948
when 'Checkpoint' then 949
when 'Chinese' then 950
when 'Chronos' then 951
when 'CI' then 952
when 'CITRIX' then 953
when 'Clementine' then 954
when 'CO' then 955
when 'COBOL' then 956
when 'Cognos' then 957
when 'Cold Fusion' then 958
when 'Compensation' then 959
when 'Compliance Consultant' then 960
when 'COMPONENT INTERFACE' then 961
when 'Consultancy CRM' then 962
when 'Copywriting' then 963
when 'CORBA' then 964
when 'Cordova' then 965
when 'CROSS-PLATFORM' then 966
when 'Crystal' then 967
when 'CS' then 968
when 'CSS' then 969
when 'Customer Service CRM Module' then 970
when 'Customer Success Manager' then 971
when 'Cyber Security' then 972
when 'Data Architecture' then 973
when 'Data Centre' then 974
when 'Data mining' then 975
when 'Data Modelling' then 976
when 'Database Administrator' then 977
when 'Database Design' then 978
when 'Database Development' then 979
when 'Datamining' then 980
when 'DB2' then 981
when 'Debian' then 982
when 'Dell Quest One' then 983
when 'Delphi' then 984
when 'Demandware' then 985
when 'Developer' then 986
when 'DHCP' then 987
when 'Digital Payment Consultant' then 988
when 'Discoverer' then 989
when 'DM' then 990
when 'DNS' then 991
when 'DP' then 992
when 'Dutch' then 993
when 'Dynamics' then 994
when 'Dynamics Technical' then 995
when 'Dynasight' then 996
when 'EAI' then 997
when 'ECM' then 998
when 'Ecommerce' then 999
when 'e-commerce' then 1000
when 'E-commerce Development' then 1001
when 'eCommunication' then 1002
when 'eCompensation' then 1003
when 'EDI' then 1004
when 'eEnergy' then 1005
when 'EH&S' then 1006
when 'ElasticSearch' then 1007
when 'ELM' then 1008
when 'ePerformance' then 1009
when 'epharma' then 1010
when 'EPM' then 1011
when 'eProcurement' then 1012
when 'eProfile' then 1013
when 'Erlang' then 1014
when 'eSourcing' then 1015
when 'ESX' then 1016
when 'Etraining' then 1017
when 'Ets' then 1018
when 'Exchange' then 1019
when 'Ext JS' then 1020
when 'F&R' then 1021
when 'FI' then 1022
when 'Finance Business Transformation' then 1023
when 'Financials' then 1024
when 'Finnish' then 1025
when 'Fintech' then 1026
when 'fiori' then 1027
when 'French' then 1028
when 'Front-End Developer' then 1029
when 'FS-CS' then 1030
when 'Functional' then 1031
when 'Functional Architect' then 1032
when 'Functional CRM' then 1033
when 'Fusion' then 1034
when 'Genio' then 1035
when 'German' then 1036
when 'Gl' then 1037
when 'Google Apps' then 1038
when 'Google Cloud Platform' then 1039
when 'Governance' then 1040
when 'GRC' then 1041
when 'Greek' then 1042
when 'Groovy' then 1043
when 'GTA' then 1044
when 'Hadoop' then 1045
when 'HANA' then 1046
when 'Hebrew' then 1047
when 'Helpdesk' then 1048
when 'Hitachi' then 1049
when 'Hotonworks' then 1050
when 'HP-UX' then 1051
when 'HR' then 1052
when 'HR ACCESS' then 1053
when 'HR Access Functional' then 1054
when 'HR Access Technical' then 1055
when 'HR AX Module' then 1056
when 'HR Business Transformation' then 1057
when 'HRIS Project Manager' then 1058
when 'HUM' then 1059
when 'Hungarian' then 1060
when 'Hybris' then 1061
when 'Hyperion' then 1062
when 'IBM cloud computing' then 1063
when 'Identity Access Management' then 1064
when 'IDOC' then 1065
when 'IDS' then 1066
when 'IEXPENSES' then 1067
when 'Ile de France' then 1068
when 'Indian' then 1069
when 'Infor M3' then 1070
when 'Informatica' then 1071
when 'Infrastructure' then 1072
when 'Integration Broker' then 1073
when 'INTERSHOP' then 1074
when 'ios developer' then 1075
when 'IP' then 1076
when 'iProcess' then 1077
when 'iProcurement' then 1078
when 'iRecruitment' then 1079
when 'IS' then 1080
when 'IS-Auto' then 1081
when 'IS-FS' then 1082
when 'IS-Mill' then 1083
when 'IS-OIL' then 1084
when 'IS-Retail' then 1085
when 'IT Security Recruitment Consultant' then 1086
when 'J2EE' then 1087
when 'Jahia' then 1088
when 'Japanese' then 1089
when 'Java' then 1090
when 'Java Project Manager' then 1091
when 'jQuery' then 1092
when 'Juniper' then 1093
when 'Junit' then 1094
when 'Kanban' then 1095
when 'LAN' then 1096
when 'Latvian' then 1097
when 'Linux' then 1098
when 'Logistics' then 1099
when 'Lotus Notes' then 1100
when 'LSMW' then 1101
when 'MAC' then 1102
when 'Magento' then 1103
when 'Managing Director' then 1104
when 'Manufacturing' then 1105
when 'Manufacturing AX Module' then 1106
when 'MapReduce' then 1107
when 'MDM' then 1108
when 'Mega Data' then 1109
when 'Meteor' then 1110
when 'Microgen' then 1111
when 'Microsoft Azure' then 1112
when 'Microsoft BI' then 1113
when 'Microsoft CRM BA' then 1114
when 'Microsoft CRM PM' then 1115
when 'Microsoft Dynamics AX Developer' then 1116
when 'Microsoft Dynamics AX Functional Consultant' then 1117
when 'Microsoft Dynamics AX Pre-Sales' then 1118
when 'Microsoft Dynamics AX Sales' then 1119
when 'Microsoft Dynamics AX Solutions Architect' then 1120
when 'Microsoft Dynamics AX Support' then 1121
when 'Microsoft Dynamics AX Test Analyst' then 1122
when 'Microsoft Dynamics NAV Functional' then 1123
when 'Microsoft Dynamics NAV PM' then 1124
when 'Microsoft Dynamics NAV Sales' then 1125
when 'Microsoft Dynamics NAV Support' then 1126
when 'Microsoft Dynamics NAV Trainer' then 1127
when 'MicroStrategy' then 1128
when 'Middle East' then 1129
when 'MM' then 1130
when 'Mobile Development' then 1131
when 'Movex' then 1132
when 'MQ Series' then 1133
when 'MRO' then 1134
when 'MUREX ARCHITECT' then 1135
when 'NETSUITE' then 1136
when 'Netweaver' then 1137
when 'Networker' then 1138
when 'Noc' then 1139
when 'Node JS' then 1140
when 'Nvision' then 1141
when 'OAF' then 1142
when 'OBIEE' then 1143
when 'Objective-C' then 1144
when 'Ocs' then 1145
when 'oData' then 1146
when 'OFA' then 1147
when 'OIC' then 1148
when 'OIL SSR' then 1149
when 'OIL TD' then 1150
when 'OIM' then 1151
when 'OKE' then 1152
when 'OLAP' then 1153
when 'OO Programming' then 1154
when 'OPEN' then 1155
when 'OpenStack' then 1156
when 'optimization' then 1157
when 'Oracle EBS Administrator' then 1158
when 'Oracle EBS CRM Business Analyst' then 1159
when 'Oracle EBS CRM Support Consultant' then 1160
when 'Oracle EBS CRM Technical Analyst' then 1161
when 'Oracle EBS Financials Business Analyst' then 1162
when 'Oracle EBS Financials Project Manager' then 1163
when 'Oracle EBS Financials Technical Analyst' then 1164
when 'Oracle EBS HRMS Functional Consultant' then 1165
when 'Oracle EBS HRMS Project Director' then 1166
when 'Oracle EBS Logistics Project Manager' then 1167
when 'Oracle EBS Logistics Support Consultant' then 1168
when 'Oracle EBS Trainer' then 1169
when 'Oracle Fusion' then 1170
when 'Oracle Fusion HCM Functional Consultant' then 1171
when 'Oracle Fusion HCM Technical Consultant' then 1172
when 'Oracle Fusion PPM' then 1173
when 'Oracle SOA Developer' then 1174
when 'Oracle SOA Project Manager' then 1175
when 'Oracle SOA Systems Administrator' then 1176
when 'Order Capture' then 1177
when 'Order Management' then 1178
when 'OSS Consultant' then 1179
when 'OSS Solution Architect' then 1180
when 'Other' then 1181
when 'Payment Consultant' then 1182
when 'Payment Processing' then 1183
when 'Payroll' then 1184
when 'Pega Architect' then 1185
when 'Pega CBA' then 1186
when 'Pega CCA' then 1187
when 'Pega CSSA' then 1188
when 'PEGA LSA' then 1189
when 'Pega PRPC' then 1190
when 'Penthao' then 1191
when 'Peoplecode' then 1192
when 'PeopleSoft' then 1193
when 'Peoplesoft Administrator' then 1194
when 'PeopleSoft Campus Techno Functional' then 1195
when 'PeopleSoft CRM Developer' then 1196
when 'PeopleSoft EPM Developer' then 1197
when 'PeopleSoft EPM Functional' then 1198
when 'PeopleSoft ESA Developer' then 1199
when 'PeopleSoft ESA Project Manager' then 1200
when 'PeopleSoft Financials Project Manager' then 1201
when 'PeopleSoft FSCM Techno Functional' then 1202
when 'PeopleSoft HR Functional' then 1203
when 'PeopleSoft Payroll Developer' then 1204
when 'PeopleSoft Project Director' then 1205
when 'PEOPLESOFT TRAINER' then 1206
when 'PERL' then 1207
when 'Persian' then 1208
when 'phonegap' then 1209
when 'PIG' then 1210
when 'PL/SQL' then 1211
when 'PLM' then 1212
when 'PM' then 1213
when 'PM/Program Manager AX' then 1214
when 'PM/Program Manager CRM' then 1215
when 'PMO' then 1216
when 'Po' then 1217
when 'POS DM' then 1218
when 'Post Merger Integration Consultant' then 1219
when 'Post Production Engineer' then 1220
when 'PP' then 1221
when 'Predictive Analytics' then 1222
when 'Private Cloud' then 1223
when 'Procurement' then 1224
when 'Product Owner' then 1225
when 'Programme Director' then 1226
when 'Prolog' then 1227
when 'PS' then 1228
when 'PS-Query' then 1229
when 'Public Cloud' then 1230
when 'QlikView' then 1231
when 'QTP' then 1232
when 'Quality Engineer' then 1233
when 'R' then 1234
when 'R.' then 1235
when 'RDJ' then 1236
when 'React JS' then 1237
when 'ReactJS' then 1238
when 'Remedy' then 1239
when 'Rendezvous' then 1240
when 'Reports' then 1241
when 'Responsive Development' then 1242
when 'RESTFUL' then 1243
when 'Retail' then 1244
when 'Risk Compliance Consultant' then 1245
when 'Risk EAD' then 1246
when 'Risk IFRS9' then 1247
when 'Risk market frtb' then 1248
when 'RM' then 1249
when 'Ruby on Rails' then 1250
when 'Russian' then 1251
when 'SAAS' then 1252
when 'Sage 100' then 1253
when 'Sage CRM' then 1254
when 'Sales' then 1255
when 'Sales CRM Module' then 1256
when 'Sales Director' then 1257
when 'Salesforce' then 1258
when 'Salesforce.com Architect' then 1259
when 'Salesforce.com Technical' then 1260
when 'SAP Adapter' then 1261
when 'SAP APO Techno/Fun Consultant' then 1262
when 'SAP BASIS Administrator/Consultant' then 1263
when 'SAP BCA Techno/Fun Consultant' then 1264
when 'SAP BI Techno-Functional Consultant' then 1265
when 'SAP BI-IP Consultant' then 1266
when 'SAP BI-IP Techno/Fun Consultant' then 1267
when 'SAP BO Consultant' then 1268
when 'SAP BO Techno/Fun Consultant' then 1269
when 'Sap Business Analyst' then 1270
when 'Sap Bw Developer' then 1271
when 'Sap Crm Consultant' then 1272
when 'SAP CTRM Consultant' then 1273
when 'SAP CTRM Techno/Functional Consultant' then 1274
when 'SAP DM Consultant' then 1275
when 'SAP DM Techno/Fun Consultant' then 1276
when 'SAP EHS Consultant' then 1277
when 'SAP EWM Techno/Fun Consultant' then 1278
when 'SAP FI-AA Consultant' then 1279
when 'SAP FI-CA Consultant' then 1280
when 'SAP Fiori' then 1281
when 'SAP FM Consultant' then 1282
when 'SAP FS-BP Consultant' then 1283
when 'SAP FS-CM Consultant' then 1284
when 'SAP FS-ICM Consultant' then 1285
when 'SAP GRC Consultant' then 1286
when 'SAP GRC Techno/Fun Consultant' then 1287
when 'SAP GTS Consultant' then 1288
when 'SAP GTS Techno/Fun Consultant' then 1289
when 'SAP HANA Consultant' then 1290
when 'SAP HCM Business Analyst' then 1291
when 'SAP HCM Consultant' then 1292
when 'SAP HCM Developer' then 1293
when 'SAP HUM Consultant' then 1294
when 'SAP IPM' then 1295
when 'SAP IS-Bev Consultant' then 1296
when 'SAP IS-EH&S Consultant' then 1297
when 'SAP IS-H Consultant' then 1298
when 'SAP IS-H Techno/Fun Consultant' then 1299
when 'SAP IS-Media Consultant' then 1300
when 'SAP IS-OIL CONSULTANT' then 1301
when 'SAP IS-Oil Techno/Fun Consultant' then 1302
when 'SAP IS-Procurement Techno/Functional Consultant' then 1303
when 'SAP IS-PS Consultant' then 1304
when 'SAP IS-T Consultant' then 1305
when 'SAP IS-U Techno/Fun Consultant' then 1306
when 'SAP JAVA Developer' then 1307
when 'SAP MDM Consultant' then 1308
when 'SAP MDM Techno/Fun Consultant' then 1309
when 'SAP MM Consultant' then 1310
when 'SAP PI Developer' then 1311
when 'SAP PLM Consultant' then 1312
when 'SAP PLM Techno/Fun Consultant' then 1313
when 'SAP PM CONSULTANT' then 1314
when 'SAP Portal Developer' then 1315
when 'SAP PP Consultant' then 1316
when 'SAP Project Manager' then 1317
when 'SAP PS Techno/Fun Consultant' then 1318
when 'SAP RE Techno/Fun Consultant' then 1319
when 'SAP RM Techno/Fun Consultant' then 1320
when 'SAP RM-CA' then 1321
when 'Sap Sd Consultant' then 1322
when 'SAP SD Techno/Fun Consultant' then 1323
when 'SAP Security' then 1324
when 'SAP Security & Authorisations Consultant' then 1325
when 'SAP SNP Techno/Functional Consultant' then 1326
when 'SAP Solution Architect' then 1327
when 'SAP Solution Manager Consultant' then 1328
when 'SAP SRM Techno/Fun Consultant' then 1329
when 'SAP Tester' then 1330
when 'SAP TM Techno/Fun Consultant' then 1331
when 'SAP Treasury Consultant' then 1332
when 'SAP TRM Consultant' then 1333
when 'SAP TRM Techno/Functional Consultant' then 1334
when 'SAP UI5' then 1335
when 'SAP VC Consultant' then 1336
when 'SAP VC Techno/Fun Consultant' then 1337
when 'SAP WM Consultant' then 1338
when 'SAP WM Techno/Fun Consultant' then 1339
when 'SASS' then 1340
when 'Sauvegarde' then 1341
when 'Scala' then 1342
when 'SCM' then 1343
when 'Scrum Master' then 1344
when 'SD' then 1345
when 'SEC Business Analayst Security' then 1346
when 'SEC cert CEH' then 1347
when 'SEC cert CISA' then 1348
when 'SEC cert COBIT' then 1349
when 'SEC cert GSEC' then 1350
when 'SEC cert ITIL' then 1351
when 'SEC cert PCI DSS' then 1352
when 'SEC Checkpoint' then 1353
when 'SEC Cisco' then 1354
when 'SEC Cloud Security' then 1355
when 'SEC Cyber Security Governance' then 1356
when 'SEC Cyber Senior Manager' then 1357
when 'SEC Digital Forensics' then 1358
when 'SEC Hacking Android' then 1359
when 'SEC IAM Business Analyst' then 1360
when 'SEC IT Service Management' then 1361
when 'SEC KPI' then 1362
when 'SEC Log Mgt' then 1363
when 'SEC MALWARE' then 1364
when 'SEC PEN TEST' then 1365
when 'SEC PKI' then 1366
when 'SEC Scada Security' then 1367
when 'SEC Security Strategy' then 1368
when 'SEC SIEM Threat Mgt' then 1369
when 'SEC SIEM Vulnerability Mgt' then 1370
when 'SEC Virtualization' then 1371
when 'SEC Web Application Security' then 1372
when 'SELLIGENT' then 1373
when 'Selligent Technical' then 1374
when 'sencha' then 1375
when 'Serbian' then 1376
when 'Service Grid' then 1377
when 'Service Manager' then 1378
when 'SHELL' then 1379
when 'Siebel' then 1380
when 'Siebel Adapter' then 1381
when 'Siebel Administrator' then 1382
when 'Siebel AMOA' then 1383
when 'Siebel Business Analyst' then 1384
when 'Siebel Developer' then 1385
when 'SIEBEL TESTER' then 1386
when 'SINATRA Framework' then 1387
when 'Slovenian' then 1388
when 'SNMP' then 1389
when 'SNP' then 1390
when 'SOAP' then 1391
when 'SOD' then 1392
when 'Solution Design' then 1393
when 'Spanish' then 1394
when 'Spark' then 1395
when 'Spotfire' then 1396
when 'Spring' then 1397
when 'SQL' then 1398
when 'SRM' then 1399
when 'SS' then 1400
when 'SSAS' then 1401
when 'Stakeholder Management' then 1402
when 'STORM' then 1403
when 'StreamServe' then 1404
when 'SuccessFactors Compensation' then 1405
when 'Successfactors Employee Central' then 1406
when 'Successfactors LMS' then 1407
when 'SuccessFactors Onboarding' then 1408
when 'Successfactors Technical Consultant' then 1409
when 'SUSE' then 1410
when 'Swift' then 1411
when 'System Administrator' then 1412
when 'System Administrator Windows' then 1413
when 'System Engineer' then 1414
when 'System Engineer Linux' then 1415
when 'T&E' then 1416
when 'Talend' then 1417
when 'Taleo' then 1418
when 'TCP/IP' then 1419
when 'TDD' then 1420
when 'Technical analyst' then 1421
when 'Technical Architect' then 1422
when 'Technical AX' then 1423
when 'Technical Lead' then 1424
when 'Technical NAV' then 1425
when 'Telco Network' then 1426
when 'Telco Support' then 1427
when 'Telecom Operational Manager' then 1428
when 'Telecom Transitional Manager' then 1429
when 'Telecoms Engineer' then 1430
when 'Telesales' then 1431
when 'Teradata' then 1432
when 'Test Manager' then 1433
when 'Tester' then 1434
when 'Tibco' then 1435
when 'TIBCO ARCHITECT' then 1436
when 'Tibco Business Analyst' then 1437
when 'Tibco Tester' then 1438
when 'Titanium' then 1439
when 'Toad' then 1440
when 'TR' then 1441
when 'Trade and Logistics AX Module' then 1442
when 'Trainer AX' then 1443
when 'Tuxedo' then 1444
when 'UML' then 1445
when 'UNIX' then 1446
when 'Update Software Functional' then 1447
when 'UX design' then 1448
when 'UX/User Experience' then 1449
when 'VC' then 1450
when 'Vietnamese' then 1451
when 'VLAN' then 1452
when 'VMWARE' then 1453
when 'VueJs 2' then 1454
when 'Waterfall' then 1455
when 'WebLogic' then 1456
when 'WebLogic Architect' then 1457
when 'Weblogic Developer' then 1458
when 'Webmethods Developer' then 1459
when 'webservices' then 1460
when 'Websphere Architect' then 1461
when 'WebSphere Project Manager' then 1462
when 'Wi-Fi' then 1463
when 'WiMax' then 1464
when 'Windows' then 1465
when 'Windows Phone' then 1466
when 'Winshuttle' then 1467
when 'Wip' then 1468
when 'Wireless' then 1469
when 'Wordpress' then 1470
when 'Workday Benefits' then 1471
when 'Workday Compensation' then 1472
when 'Workday Expenses' then 1473
when 'Workday Finance Technical Consultant' then 1474
when 'Workday French Payroll' then 1475
when 'Workday HCM Core Certified' then 1476
when 'Workday Payroll Functional Consultant' then 1477
when 'Workday Payroll Technical Consultant' then 1478
when 'Workday Project Manager' then 1479
when 'Workday Recruiting Certified' then 1480
when 'Workday Reporting Composite' then 1481
when 'Workday Testing' then 1482
when 'Workday Time and Absence' then 1483
when 'Workday Trainer' then 1484
when 'WorkFlow' then 1485
when 'WSS' then 1486
when 'Yarn' then 1487
when 'Zend' then 1488
end as 'sfe'
                from bullhorn1.Candidate C
                left join SkillName SN on C.userID = SN.userId
                where C.isPrimaryOwner = 1 and  SN.userId is not null
)
--select count(*) from sfe where sfe is not null 
select * from sfe where sfe is not null and candidateID <100
--select count(distinct ltrim(SkillName)) as Skill from SkillName --where SkillName
--select distinct ltrim(SkillName) as Skill from SkillName --where SkillName
-- select * from bullhorn1.BH_SkillList SL where name in ('Product Mgmt & Marketing','Customer/Data Analytics','Cash Ops','Investment/Portfolio Mgmt','Investment research and analysis','Credit admin/ops','Card Ops','HR Analytics','Compensation','Benefits','L&D','Robotic process automation (RPA)','AI & machine learning','JD Edwards ERP','Avaloq','Cognos','Hyperion','Bloomberg','Reuters','Matlab','Labview','Pro E+','SAS','Qlikview','Tableau','R Programming','SPSS','Mobile app developer','Assistant Manager','Senior Manager','Local','Startup','Social Insights & Analytics','Art Creative Director','Copy Art Director','Integrated Marketing','Digital media','Customer/Data Analytics','Risk & Compliance','Advisory/Sales','Investment/Portfolio Mgmt','Project Mgmt/Transformation','Client Service/Call Centre','Capex or Opex category sourcing','Chemical sourcing','Consumables category','Electrical category','Electronic component category','EMS category','Flavour category','Frangrance category','IT category sourcing','Logistic category sourcing','Marketing category sourcing','Mechanical category','NPI category sourcing','Oil & gas sourcing','Professional category sourcing','Project category sourcing','Supplier mgmt','Raw material sourcing','Reverse auction','Distribution','Media research')

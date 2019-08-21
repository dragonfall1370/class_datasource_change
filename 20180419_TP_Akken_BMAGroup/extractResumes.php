<?php
	$MYSQL_DBIP = "10.1.4.53";
	$MYSQL_USER = "root";
	$MYSQL_PASS = "123456";

	$DBNAME = "bma";

	// PATH TO CREATE THE RESUMES
	$fpath = "/tmp/resumes/"; 

	#$db=mysql_connect($MYSQL_DBIP,$MYSQL_USER,$MYSQL_PASS);
	$db=mysqli_connect($MYSQL_DBIP,$MYSQL_USER,$MYSQL_PASS);
	//mysql_select_db($DBNAME);
	mysqli_select_db($db,$DBNAME);

	$selQry = "SELECT sno, res_name, filecontent FROM con_resumes";
	//$resQry = mysql_query($selQry,$db);
	//$resQry = mysqli_query($selQry,$db);
	$resQry = mysqli_query($db,$selQry);
	//while($rowQry = mysql_fetch_row($resQry))
	while($rowQry = mysqli_fetch_row($resQry))
	{
		$contents=$rowQry[2];

		// THIS WILL CREATE THE FILE USING RESUME ID. IF WE WOULD LIKE TO CREATE USING THE FILE NAME CHANGE THIS TO $rowQry[1]. RECOMMENDED USING SNO SO THAT IT IS UNIQUE AND ALSO CAN BUILD RELATION TO THE CANDIDATE EASILY. THE RELATION IS con_resumes.sno = candidate_list.resid 
		$filename=$rowQry[0];

		$fp=fopen($fpath.$filename,"w");
		fwrite($fp,$contents);
		fclose($fp);
	}

	//mysql_close($db);
	mysqli_close($db);
?>

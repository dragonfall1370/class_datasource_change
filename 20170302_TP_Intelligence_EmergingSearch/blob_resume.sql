#CREATE DEFINER=`linux`@`%` PROCEDURE `testing`()
CREATE DEFINER=`root`@`localhost` PROCEDURE `resume_procedure`()
BEGIN

    declare this_id int;    
    declare ext varchar(10);
    declare cur1 cursor for select ID,FileExtention from canfiles where FileExtention is not null and FileExtention not like '% %';    
    open cur1;
      read_loop: loop
        fetch cur1 into this_id,ext;
        
        set @query = concat('select FileBlob from canfiles where ID=', this_id, ' into dumpfile "D:emergingsearch/', this_id,'.',ext,'"');
        select @query;
        prepare write_file from @query;
        execute write_file;  
      end loop read_loop;  
     close cur1;
END
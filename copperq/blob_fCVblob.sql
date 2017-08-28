drop procedure if exists fcvblob_procedure;
delimiter //

CREATE DEFINER=`root`@`localhost` PROCEDURE `fcvblob_procedure`()
BEGIN
    declare this_id int;    
    declare name varchar(1000);
    declare cur1 cursor for select ID,FileName from fcvblob;    
    open cur1;
      read_loop: loop
        fetch cur1 into this_id,name;
        set @query = concat('select FileBlob from fcvblob where ID=', this_id, ' into dumpfile "D:/emergingsc/fcvblob/',this_id,'.',name,'"');
        #select @query;
        prepare write_file from @query;
        execute write_file;  
      end loop read_loop;  
     close cur1;
END
drop procedure if exists photo_procedure;
delimiter //

CREATE DEFINER=`root`@`localhost` PROCEDURE `photo_procedure`()
BEGIN
    declare this_id int;
    declare cur1 cursor for select CanID from photoblob;

    open cur1;
      read_loop: loop
        fetch cur1 into this_id;
        set @query = concat('select FileBlob from photoblob where CanID=', this_id, ' into dumpfile "D:/emergingsearch/photo/', this_id,'.jpg"');
        prepare write_file from @query;
        execute write_file;
      end loop;
    close cur1;

  END

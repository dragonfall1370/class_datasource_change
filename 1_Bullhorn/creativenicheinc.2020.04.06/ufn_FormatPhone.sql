DROP FUNCTION [dbo].[ufn_FormatPhone];
CREATE FUNCTION [dbo].[ufn_FormatPhone] (@PhoneNumber VARCHAR(32))
RETURNS VARCHAR(32)
AS
BEGIN
    DECLARE @Phone CHAR(32)
--    DECLARE @Phone1 CHAR(32)

--    SET @Phone1 = @PhoneNumber
    SET @Phone = @PhoneNumber

    -- cleanse phone number string
    WHILE PATINDEX('%[^0-9]%', @PhoneNumber) > 0
        SET @PhoneNumber = REPLACE(REPLACE(REPLACE(@PhoneNumber, SUBSTRING(@PhoneNumber, PATINDEX('%[^0-9]%', @PhoneNumber), 1), ''),'-',''),' ','')



    IF LEN(@PhoneNumber) = 7 --AND LEFT(@PhoneNumber, 1) <> '0'
        BEGIN
        --SET @Phone = @PhoneNumber
        SET @PhoneNumber = '+1416' + SUBSTRING(@PhoneNumber, 1, 7) --+ @PhoneNumber --SET @PhoneNumber = @PhoneNumber + ' X' + SUBSTRING(@Phone, 11, LEN(@Phone) - 10)
--        SET @Phone1 = '+1416 ' + SUBSTRING(@PhoneNumber, 1, 7) --+ @PhoneNumber --SET @PhoneNumber = @PhoneNumber + ' X' + SUBSTRING(@Phone, 11, LEN(@Phone) - 10)
        RETURN @PhoneNumber
        END

        
    -- build US standard phone number
    IF LEN(@PhoneNumber) = 10
        BEGIN
        --SET @Phone = @PhoneNumber
        --SET @PhoneNumber = '(' + SUBSTRING(@PhoneNumber, 1, 3) + ') ' + SUBSTRING(@PhoneNumber, 4, 3) + '-' + SUBSTRING(@PhoneNumber, 7, 4)
        SET @PhoneNumber = '+1' + SUBSTRING(@PhoneNumber, 1, 3) + '' + SUBSTRING(@PhoneNumber, 4, 3) + '' + SUBSTRING(@PhoneNumber, 7, 4)
--        SET @Phone1 = '+1 ' + SUBSTRING(@PhoneNumber, 1, 3) + '' + SUBSTRING(@PhoneNumber, 4, 3) + '' + SUBSTRING(@PhoneNumber, 7, 4)
        RETURN @PhoneNumber
        END

    IF LEN(@PhoneNumber) = 11 AND LEFT(@PhoneNumber, 1) = '1'
        BEGIN
        --SET @Phone = @PhoneNumber
        --SET @PhoneNumber = '(' + SUBSTRING(@PhoneNumber, 1, 3) + ') ' + SUBSTRING(@PhoneNumber, 4, 3) + '-' + SUBSTRING(@PhoneNumber, 7, 4)
        SET @PhoneNumber = '+' + SUBSTRING(@PhoneNumber, 1, 4) + '' + SUBSTRING(@PhoneNumber, 5, 3) + '' + SUBSTRING(@PhoneNumber, 8, 4)
--        SET @Phone1 = '+1 ' + SUBSTRING(@PhoneNumber, 1, 3) + '' + SUBSTRING(@PhoneNumber, 4, 3) + '' + SUBSTRING(@PhoneNumber, 7, 4)
        RETURN @PhoneNumber
        END
        
    -- skip foreign phones
--    IF LEN(@PhoneNumber) > 11
--       AND (
--            SUBSTRING(@PhoneNumber, 1, 1) = '1'
--            OR SUBSTRING(@PhoneNumber, 1, 1) = '+'
--            OR SUBSTRING(@PhoneNumber, 1, 1) = '0'
--            ) 
--        RETURN @Phone

--    IF LEN(@PhoneNumber) < 7
--        RETURN @Phone    
--    IF LEN(@PhoneNumber) > 7 and LEN(@PhoneNumber) < 10
--        RETURN @Phone
--    IF LEN(@PhoneNumber) > 10
--        RETURN @Phone
       
    RETURN @Phone
END
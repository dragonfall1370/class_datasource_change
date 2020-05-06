CREATE FUNCTION [dbo].[FormatPhoneNumber] (
    @PhoneNumber VARCHAR(50),
    @DefaultIfUnknown VARCHAR(50)
)
RETURNS VARCHAR(50)
AS
BEGIN
    -- remove any extension
    IF CHARINDEX('x', @PhoneNumber, 1) > 0
        SET @PhoneNumber = SUBSTRING(@PhoneNumber, 1, CHARINDEX('x', @PhoneNumber, 1) - 1)

    -- cleanse phone number string
    WHILE PATINDEX('%[^0-9]%',@PhoneNumber) > 0
        SET @PhoneNumber = REPLACE(@PhoneNumber,
                SUBSTRING(@PhoneNumber,PATINDEX('%[^0-9]%',@PhoneNumber),1),'')

    -- Remove US international code if exists, i.e. 12345678900
    IF SUBSTRING(@PhoneNumber,1,1) = '1' AND LEN(@PhoneNumber) = 11
        SET @PhoneNumber = SUBSTRING(@PhoneNumber, 2, 10)

    -- any phone numbers without 10 characters are set to default
    IF LEN(@PhoneNumber) <> 10
        RETURN @DefaultIfUnknown

    -- build US standard phone number
    SET @PhoneNumber = '(' + SUBSTRING(@PhoneNumber,1,3) + ') ' +
                SUBSTRING(@PhoneNumber,4,3) + '-' + SUBSTRING(@PhoneNumber,7,4)

    RETURN @PhoneNumber
END
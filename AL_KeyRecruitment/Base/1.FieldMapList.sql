select 
         entity
        , columnName
        , display
        , editType
        , isRequired
        , isHidden
        , valueList
        , allowMultiple
        , description
        , hint
        , defaultValue
        , isHidden
from bullhorn1.BH_FieldMapList 
--where isHidden = 0
--where isRequired = 1
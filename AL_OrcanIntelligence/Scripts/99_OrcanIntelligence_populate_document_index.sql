select
Id,
concat(Id, '-',
	replace(
		replace(trim(isnull([Name], '')), ',', '_')
		, ' ', '_'
	)
) as Doc
from Attachment
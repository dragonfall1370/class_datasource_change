select 
a.cid,
concat(
'Direct Hire: ',iif(placedirecthire = 1,'Yes','No'),(char(13)+char(10)),
'Date Accepted: ',placedatemade,(char(13)+char(10)),
nullif(concat('Start Date: ',Startdate,(char(13)+char(10))),concat('Start Date: ',(char(13)+char(10)))),
nullif(concat('End Date: ',placecontestenddate,(char(13)+char(10))),concat('End Date: ',(char(13)+char(10)))),
'Bill Rate: ',placecontbill,(char(13)+char(10)),
'Pay Rate: ',placecontpay,(char(13)+char(10)),
'Hourly Burden: ',PlaceContBurdenPerHour,(char(13)+char(10)),
'Est.Hour: ',placecontesthours,(char(13)+char(10)),
'Hourly Margin: ',PlaceContMarginPerHour,(char(13)+char(10)),
'Est.Revenue: ',placecontestrevenue,(char(13)+char(10))

) as Note
from place a
left join company b on a.id = b.id
left join jobs c on a.reference = c.reference
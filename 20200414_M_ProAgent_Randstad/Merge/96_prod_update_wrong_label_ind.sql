---UDPATE LABEL FROM VINCERE
select *
from vertical
where parent_id = 29029 --id=29183

update vertical
set name = 'Infra, Datacenter, Cloud (IaaS/PaaS) / インフラ, データセンター, クラウド（laaS/PaaS）'
where id=29183
and parent_id = 29029

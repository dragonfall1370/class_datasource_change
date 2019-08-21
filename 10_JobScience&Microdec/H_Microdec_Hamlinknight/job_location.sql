WITH job_location AS (
	SELECT 
		opportunity_ref,
		CASE
			WHEN l.description = 'INTERNATIONAL' THEN 1
			WHEN l.description = 'Berlin' THEN 2
			WHEN l.description = 'Denmark' THEN 3
			WHEN l.description = 'Luxembourg' THEN 4
			WHEN l.description = 'NORTH WEST' THEN 5
			WHEN l.description = 'Manchester' THEN 6
			WHEN l.description = 'Oldham' THEN 7
			WHEN l.description = 'London' THEN 8
			WHEN l.description = 'Northampton' THEN 9
			WHEN l.description = 'Iceland' THEN 10
			WHEN l.description = 'Iver' THEN 11
			WHEN l.description = 'Dunstable' THEN 12
			WHEN l.description = 'Luton' THEN 13
			WHEN l.description = 'Milton Keynes' THEN 14
			WHEN l.description = 'Bedfordshire' THEN 15
			WHEN l.description = 'Luton' THEN 16
			WHEN l.description = 'Essex' THEN 17
			WHEN l.description = 'HERTFORDSHIRE' THEN 18
			WHEN l.description = 'Borehamwood' THEN 19
			WHEN l.description = 'Bushey' THEN 20
			WHEN l.description = 'Chorleywood' THEN 21
			WHEN l.description = 'Hatfield' THEN 22
			WHEN l.description = 'Hemel Hempstead' THEN 23
			WHEN l.description = 'Hitchin' THEN 24
			WHEN l.description = 'Maple Cross' THEN 25
			WHEN l.description = 'Rickmansworth' THEN 26
			WHEN l.description = 'St Albans' THEN 27
			WHEN l.description = 'Stevenage' THEN 28
			WHEN l.description = 'Watford' THEN 29
			WHEN l.description = 'Welwyn Garden City' THEN 30
			WHEN l.description = 'Kings Langley' THEN 31
			WHEN l.description = 'Chipperfield' THEN 32
			WHEN l.description = 'Bovingdon' THEN 33
			WHEN l.description = 'Berkhamstead' THEN 34
			WHEN l.description = 'Tring' THEN 35
			WHEN l.description = 'Redbourne' THEN 36
			WHEN l.description = 'Wembley' THEN 37
			WHEN l.description = 'Elstree' THEN 38
			WHEN l.description = 'Radlett' THEN 39
			WHEN l.description = 'Hertfordshire' THEN 40
			WHEN l.description = 'BUCKINGHAMSHIRE' THEN 41
			WHEN l.description = 'The Chalfonts' THEN 42
			WHEN l.description = 'Denham' THEN 43
			WHEN l.description = 'Gerrards Cross' THEN 44
			WHEN l.description = 'Stoke Poges' THEN 45
			WHEN l.description = 'High Wycombe' THEN 46
			WHEN l.description = 'Amersham' THEN 47
			WHEN l.description = 'Chesham' THEN 48
			WHEN l.description = 'Marlow' THEN 49
			WHEN l.description = 'Buckinghamshire' THEN 50
			WHEN l.description = 'MIDDLESEX' THEN 51
			WHEN l.description = 'Cowley' THEN 52
			WHEN l.description = 'Greenford' THEN 53
			WHEN l.description = 'Harefield' THEN 54
			WHEN l.description = 'Harmondsworth' THEN 55
			WHEN l.description = 'Harrow' THEN 56
			WHEN l.description = 'Hayes' THEN 57
			WHEN l.description = 'Heathrow' THEN 58
			WHEN l.description = 'Heston' THEN 59
			WHEN l.description = 'Hillingdon' THEN 60
			WHEN l.description = 'Ickenham' THEN 61
			WHEN l.description = 'Northolt' THEN 62
			WHEN l.description = 'Ruislip' THEN 63
			WHEN l.description = 'Southall' THEN 64
			WHEN l.description = 'Stanmore' THEN 65
			WHEN l.description = 'Stockley Park' THEN 66
			WHEN l.description = 'Uxbridge' THEN 67
			WHEN l.description = 'West Drayton' THEN 68
			WHEN l.description = 'Yiewsley' THEN 69
			WHEN l.description = 'Middlesex' THEN 70
			WHEN l.description = 'East Sussex' THEN 71
			WHEN l.description = 'Staines' THEN 72
			WHEN l.description = 'KENT' THEN 73
			WHEN l.description = 'Bromley' THEN 74
			WHEN l.description = 'Kent' THEN 75
			WHEN l.description = 'SURREY' THEN 76
			WHEN l.description = 'BERKSHIRE' THEN 77
			WHEN l.description = 'Colnbrook' THEN 78
			WHEN l.description = 'Langley' THEN 79
			WHEN l.description = 'Poyle' THEN 80
			WHEN l.description = 'Slough' THEN 81
			WHEN l.description = 'Berkshire' THEN 82
			WHEN l.description = 'Windsor' THEN 83
			WHEN l.description = 'OXFORDSHIRE' THEN 84
			WHEN l.description = 'Oxfordshire' THEN 85
			WHEN l.description = 'Banbury' THEN 86
			WHEN l.description = 'Bicester' THEN 87
			WHEN l.description = 'Thame' THEN 88
			WHEN l.description = 'Wiltshire' THEN 89
			WHEN l.description = 'LONDON' THEN 90
			WHEN l.description = 'City of London' THEN 91
			WHEN l.description = 'East London' THEN 92
			WHEN l.description = 'Stratford' THEN 93
			WHEN l.description = 'Docklands' THEN 94
			WHEN l.description = 'North London' THEN 95
			WHEN l.description = 'South London' THEN 96
			WHEN l.description = 'West London' THEN 97
			WHEN l.description = 'SOUTH EAST' THEN 98
			WHEN l.description = 'West Sussex' THEN 99
			WHEN l.description = 'Hampshire' THEN 100
			WHEN l.description = 'SOUTH WEST' THEN 101
			WHEN l.description = 'Bristol' THEN 102
			WHEN l.description = 'Avon' THEN 103
			WHEN l.description = 'Cornwall' THEN 104
			WHEN l.description = 'Dorset' THEN 105
			WHEN l.description = 'Devon' THEN 106
			WHEN l.description = 'Gloucestershire' THEN 107
			WHEN l.description = 'Somerset' THEN 108
			WHEN l.description = 'Cambridgeshire' THEN 109
			WHEN l.description = 'Norfolk' THEN 110
			WHEN l.description = 'Suffolk' THEN 111
			WHEN l.description = 'WEST MIDLANDS' THEN 112
			WHEN l.description = 'West Midlands' THEN 113
			WHEN l.description = 'Birmingham' THEN 114
			WHEN l.description = 'Solihull' THEN 115
			WHEN l.description = 'Dudley' THEN 116
			WHEN l.description = 'Herefordshire' THEN 117
			WHEN l.description = 'Worcestershire' THEN 118
			WHEN l.description = 'Staffordshire' THEN 119
			WHEN l.description = 'WARWICKSHIRE' THEN 120
			WHEN l.description = 'Atherstone' THEN 121
			WHEN l.description = 'Bedworth' THEN 122
			WHEN l.description = 'Coventry' THEN 123
			WHEN l.description = 'Kenilworth' THEN 124
			WHEN l.description = 'Leamington Spa' THEN 125
			WHEN l.description = 'Nuneaton' THEN 126
			WHEN l.description = 'Rugby' THEN 127
			WHEN l.description = 'Stratford Upon Avon' THEN 128
			WHEN l.description = 'Warwick' THEN 129
			WHEN l.description = 'Southam' THEN 130
			WHEN l.description = 'Warwickshire' THEN 131
			WHEN l.description = 'Lutterworth' THEN 132
			WHEN l.description = 'NORTH EAST' THEN 133
			WHEN l.description = 'NORTH YORKSHIRE' THEN 134
			WHEN l.description = 'North Yorkshire' THEN 135
			WHEN l.description = 'SOUTH YORKSHIRE' THEN 136
			WHEN l.description = 'South Yorkshire' THEN 137
			WHEN l.description = 'Sheffield' THEN 138
			WHEN l.description = 'Worksop' THEN 139
			WHEN l.description = 'Rotherham' THEN 140
			WHEN l.description = 'Mansfield' THEN 141
			WHEN l.description = 'Doncaster' THEN 142
			WHEN l.description = 'Derby' THEN 143
			WHEN l.description = 'Barnsley' THEN 144
			WHEN l.description = 'Chesterfield' THEN 145
			WHEN l.description = 'Dronfield' THEN 146
			WHEN l.description = 'Pontefract' THEN 147
			WHEN l.description = 'WEST YORKSHIRE' THEN 148
			WHEN l.description = 'Leeds' THEN 149
			WHEN l.description = 'EAST YORKSHIRE' THEN 150
			WHEN l.description = 'Goole' THEN 151
			WHEN l.description = 'Lincoln' THEN 152
			WHEN l.description = 'Grimsby' THEN 153
			WHEN l.description = 'Hull' THEN 154
			WHEN l.description = 'Scunthorpe' THEN 155
			WHEN l.description = 'SCOTLAND' THEN 156
			WHEN l.description = 'Aberdeen' THEN 157
			WHEN l.description = 'Dundee' THEN 158
			WHEN l.description = 'Edinburgh' THEN 159
			WHEN l.description = 'Fife' THEN 160
			WHEN l.description = 'Glasgow' THEN 161
			WHEN l.description = 'Kilmarnock' THEN 162
			WHEN l.description = 'Perth' THEN 163
			WHEN l.description = 'Sterling' THEN 164
			WHEN l.description = 'Scotland' THEN 165
			WHEN l.description = 'WALES' THEN 166
			WHEN l.description = 'Wales' THEN 167
			WHEN l.description = 'Cardiff' THEN 168
			WHEN l.description = 'Clywd' THEN 169
			WHEN l.description = 'Dyfed' THEN 170
			WHEN l.description = 'Gwynedd' THEN 171
			WHEN l.description = 'Gwent' THEN 172
			WHEN l.description = 'Mid Glamorgan' THEN 173
			WHEN l.description = 'South Glamorgan' THEN 174
			WHEN l.description = 'EAST MIDLANDS' THEN 175
			WHEN l.description = 'Leicestershire' THEN 176
			WHEN l.description = 'Hinckley' THEN 177
			WHEN l.description = 'Derby' THEN 178
			WHEN l.description = 'Nottingham' THEN 179
		END job_location,
		ROW_NUMBER() OVER(PARTITION BY opportunity_ref ORDER BY sc.update_timestamp DESC) rn
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1020'
	AND l.description IS NOT NULL
),
concat_job_location AS (
	SELECT
		opportunity_ref,
		string_agg(job_location::TEXT, ',') job_location
	FROM job_location
	WHERE rn = 1
	GROUP BY opportunity_ref
)
------------------------------------------------- Main query --------------------------------------------------------------------------
SELECT
o.opportunity_ref AS job_id,
job_location,
'add_job_info' additional_type,
1007 form_id,
11275 field_id,
'11275' constraint_id,
CURRENT_TIMESTAMP insert_timestamp
FROM opportunity o
JOIN concat_job_location jl ON o.opportunity_ref = jl.opportunity_ref
--CANDIDATE RS OFFICE > BRANCH | #CF: 11313
insert into branch_record (record_id, record_type, branch_id, insert_timestamp) 
select additional_id as record_id
, 'job' as record_type
--, field_id
, case field_value::int
	when 1 then 1141
	when 2 then 1154
	when 3 then 1149
	when 4 then 1138
	when 5 then 1129
	when 6 then 1180
	when 7 then 1171
	when 8 then 1151
	when 9 then 1135
	when 10 then 1179
	when 11 then 1167
	when 12 then 1176
	when 13 then 1143
	when 14 then 1174
	when 15 then 1150
	when 16 then 1170
	when 17 then 1137
	when 18 then 1166
	when 19 then 1139
	when 20 then 1132
	when 21 then 1147
	when 22 then 1146
	when 23 then 1172
	when 24 then 1165
	when 25 then 1169
	when 26 then 1140
	when 27 then 1173
	when 28 then 1175
	when 29 then 1134
	when 30 then 1184
	when 31 then 1159
	when 32 then 1164
	when 33 then 1136
	when 34 then 1155
	when 35 then 1133
	when 36 then 1152
	when 37 then 1161
	when 38 then 1144
	when 39 then 1145
	when 40 then 1128
	when 41 then 1131
	when 42 then 1156
	when 43 then 1178
	when 44 then 1182
	when 45 then 1158
	when 46 then 1157
	when 47 then 1168
	when 48 then 1181
	when 49 then 1142
	when 50 then 1163
	when 51 then 1148
	when 52 then 1127
	when 53 then 1162
	when 54 then 1177
	when 55 then 1183
	when 56 then 1186
	when 57 then 1160
	when 58 then 1153
	when 59 then 1130
	when 60 then 1185
end as branch_id
, current_timestamp as insert_timestamp
from additional_form_values
where field_id = 11313
and field_value is not NULL
on conflict on constraint branch_record_branch_id_record_id_record_type_key
	do nothing
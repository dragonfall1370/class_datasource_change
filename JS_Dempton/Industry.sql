select id, b.category from company a left join categories b on a.Category_ID = b.categories_id
where category is not null
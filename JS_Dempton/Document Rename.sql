with document as (select docs_id, iif(doctitle = '' or doctitle is null,cast(docs_id as nvarchar(10)),doctitle) as doctitle , FileExt from docs)

,document2 as (select concat(docs_id,'.',FileExt) as 'docs_id',concat(docs_id,'-',replace(doctitle,',','_'),'.',FileExt) as 'filename' from document)

select concat(docs_id,'*',filename) as document from document2

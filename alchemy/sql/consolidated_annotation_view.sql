create view consolidated_annotation
as
with
weighted_annotation as
(select
	ca.entity,
	ca.value,
	ca.entity_type,
	ca."label",
	sum(coalesce(ca.weight, 1)) as weight
	from classification_annotation ca
	where ca.value != -2 and ca.value != 0
	group by ca.entity_type, ca.entity, ca."label", ca.value),
weighted_annotation_with_row_number as
(select
	wa.entity,
	wa.value,
	wa.entity_type,
	wa."label",
	wa.weight,
	row_number() over (partition by
							wa.entity_type,
							wa.entity,
							wa."label"
						order by wa.weight desc) as row_num
	from weighted_annotation as wa)
select distinct on (ca.entity_type, ca.entity, ca."label")
	wa_row_num.entity,
	wa_row_num.value,
	wa_row_num.weight,
	wa_row_num.entity_type,
	wa_row_num."label",
	ca.context->'text' as company_description
from weighted_annotation_with_row_number as wa_row_num
join classification_annotation ca
on wa_row_num.entity = ca.entity and wa_row_num.entity_type = ca.entity_type and wa_row_num."label" = ca."label" 
where wa_row_num.row_num = 1
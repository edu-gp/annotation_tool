create or replace view consolidated_annotation
as
with
weighted_annotation as (
	select
		ca.entity_type
		,ca.entity
		,ca.label
		,sum(coalesce(ca.weight, 1) * ca.value) as weighted_sum
		,count(1) as total_votes
	from classification_annotation ca
	where ca.value in (-1,1)
	group by ca.entity_type, ca.entity, ca.label
)
select * from weighted_annotation
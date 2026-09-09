
    
    

select
    archetype_index as unique_field,
    count(*) as n_records

from BALBOA.SEEDS.seed_vendor_archetypes
where archetype_index is not null
group by archetype_index
having count(*) > 1



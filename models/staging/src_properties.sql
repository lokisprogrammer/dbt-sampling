select
    id as property_id,
    name
from {{ref('properties')}}
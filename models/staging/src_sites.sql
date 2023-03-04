select
    id as site_id,
    property_id,
    type,
    price_per_night
from {{ref('sites')}}
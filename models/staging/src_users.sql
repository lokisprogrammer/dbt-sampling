select
    id as user_id,
    first_name,
    last_name,
    email,
    phone
from {{ref('users')}}
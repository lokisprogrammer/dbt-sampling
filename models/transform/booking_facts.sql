with

joined as (
    select
        b.created_at,
        b.booking_id,
        b.user_id,
        b.site_id,
        b.start_date,
        b.end_date,
        datediff('day', b.start_date, b.end_date) as no_of_nights,
        no_of_nights * s.price_per_night as total_price,
        p.name as property_name,
        rank() over (partition by b.user_id order by b.created_at) as booking_number,
        p.property_id,
        u.first_name,
        u.last_name,
        u.email,
        u.phone
    from {{ref('src_bookings')}} b
    left join {{ref('src_users')}} u
        on b.user_id = u.user_id
    left join {{ref('src_sites')}} s
        on b.site_id = s.site_id
    left join {{ref('src_properties')}} p
        on s.property_id = p.property_id
)

select * from joined

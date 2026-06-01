-- ─── Seed: orders ─────────────────────────────────────────────────────────────
INSERT INTO orders
    (order_id, customer_id, restaurant_id, city, item_name, quantity, amount, payment_mode, delivery_status, order_time)
VALUES
    (1001, 501, 12, 'Mumbai',    'Chicken Biryani',       2, 450.00, 'UPI',             'Delivered',       NOW() - INTERVAL '10 minutes'),
    (1002, 502, 45, 'Delhi',     'Paneer Butter Masala',  1, 320.00, 'Credit Card',     'Delivered',       NOW() - INTERVAL '9 minutes'),
    (1003, 503, 23, 'Bangalore', 'Masala Dosa',           3, 210.00, 'Debit Card',      'Out for Delivery',NOW() - INTERVAL '8 minutes'),
    (1004, 504, 67, 'Chennai',   'Veg Burger',            2, 280.00, 'Cash on Delivery','Pending',         NOW() - INTERVAL '7 minutes'),
    (1005, 505, 33, 'Hyderabad', 'Chicken Tikka',         1, 380.00, 'UPI',             'Delivered',       NOW() - INTERVAL '6 minutes'),
    (1006, 506, 88, 'Pune',      'Margherita Pizza',      2, 650.00, 'Credit Card',     'Delivered',       NOW() - INTERVAL '5 minutes'),
    (1007, 507, 19, 'Kolkata',   'Pav Bhaji',             4, 340.00, 'UPI',             'Cancelled',       NOW() - INTERVAL '4 minutes'),
    (1008, 508, 55, 'Ahmedabad', 'Dal Makhani',           1, 290.00, 'Debit Card',      'Delivered',       NOW() - INTERVAL '3 minutes'),
    (1009, 509, 72, 'Jaipur',    'Chole Bhature',         2, 240.00, 'Cash on Delivery','Delivered',       NOW() - INTERVAL '2 minutes'),
    (1010, 510, 41, 'Lucknow',   'Idli Sambar',           3, 180.00, 'UPI',             'Out for Delivery',NOW() - INTERVAL '1 minute'),
    (1011, 511, 14, 'Mumbai',    'Butter Chicken',        2, 520.00, 'Credit Card',     'Delivered',       NOW() - INTERVAL '55 seconds'),
    (1012, 512, 29, 'Delhi',     'Mutton Curry',          1, 480.00, 'UPI',             'Delivered',       NOW() - INTERVAL '50 seconds'),
    (1013, 513, 63, 'Bangalore', 'Veg Fried Rice',        2, 260.00, 'Debit Card',      'Pending',         NOW() - INTERVAL '45 seconds'),
    (1014, 514, 37, 'Chennai',   'Egg Fried Rice',        3, 330.00, 'Cash on Delivery','Delivered',       NOW() - INTERVAL '40 seconds'),
    (1015, 515, 81, 'Hyderabad', 'Veg Noodles',           1, 220.00, 'UPI',             'Out for Delivery',NOW() - INTERVAL '35 seconds'),
    (1016, 516, 52, 'Pune',      'Shawarma',              2, 360.00, 'Credit Card',     'Delivered',       NOW() - INTERVAL '30 seconds'),
    (1017, 517, 17, 'Kolkata',   'Aloo Paratha',          4, 200.00, 'UPI',             'Delivered',       NOW() - INTERVAL '25 seconds'),
    (1018, 518, 44, 'Ahmedabad', 'Samosa',                5, 150.00, 'Debit Card',      'Cancelled',       NOW() - INTERVAL '20 seconds'),
    (1019, 519, 76, 'Jaipur',    'Gulab Jamun',           2, 120.00, 'Cash on Delivery','Delivered',       NOW() - INTERVAL '15 seconds'),
    (1020, 520, 38, 'Lucknow',   'Rasgulla',              3, 160.00, 'UPI',             'Delivered',       NOW() - INTERVAL '10 seconds')
ON CONFLICT (order_id) DO NOTHING;

-- ─── Seed: city_sales_summary ─────────────────────────────────────────────────
INSERT INTO city_sales_summary (city, total_orders, total_revenue)
VALUES
    ('Mumbai',    2,  970.00),
    ('Delhi',     2,  800.00),
    ('Bangalore', 2,  470.00),
    ('Chennai',   2,  610.00),
    ('Hyderabad', 2,  600.00),
    ('Pune',      2, 1010.00),
    ('Kolkata',   2,  540.00),
    ('Ahmedabad', 2,  440.00),
    ('Jaipur',    2,  360.00),
    ('Lucknow',   2,  340.00)
ON CONFLICT (city) DO UPDATE SET
    total_orders  = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    last_updated  = NOW();

-- ─── Seed: food_sales_summary ─────────────────────────────────────────────────
INSERT INTO food_sales_summary (item_name, total_orders, total_revenue)
VALUES
    ('Chicken Biryani',       1, 450.00),
    ('Paneer Butter Masala',  1, 320.00),
    ('Masala Dosa',           1, 210.00),
    ('Veg Burger',            1, 280.00),
    ('Chicken Tikka',         1, 380.00),
    ('Margherita Pizza',      1, 650.00),
    ('Pav Bhaji',             1, 340.00),
    ('Dal Makhani',           1, 290.00),
    ('Chole Bhature',         1, 240.00),
    ('Idli Sambar',           1, 180.00),
    ('Butter Chicken',        1, 520.00),
    ('Mutton Curry',          1, 480.00),
    ('Veg Fried Rice',        1, 260.00),
    ('Egg Fried Rice',        1, 330.00),
    ('Veg Noodles',           1, 220.00),
    ('Shawarma',              1, 360.00),
    ('Aloo Paratha',          1, 200.00),
    ('Samosa',                1, 150.00),
    ('Gulab Jamun',           1, 120.00),
    ('Rasgulla',              1, 160.00)
ON CONFLICT (item_name) DO UPDATE SET
    total_orders  = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    last_updated  = NOW();

-- ─── Seed: payment_summary ────────────────────────────────────────────────────
INSERT INTO payment_summary (payment_mode, total_orders, total_revenue)
VALUES
    ('UPI',              9, 2950.00),
    ('Credit Card',      4, 1900.00),
    ('Debit Card',       4, 1020.00),
    ('Cash on Delivery', 3,  650.00)
ON CONFLICT (payment_mode) DO UPDATE SET
    total_orders  = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    last_updated  = NOW();

-- ─── Seed: delivery_summary ───────────────────────────────────────────────────
INSERT INTO delivery_summary (delivery_status, total_orders)
VALUES
    ('Delivered',        13),
    ('Out for Delivery',  3),
    ('Pending',           2),
    ('Cancelled',         2)
ON CONFLICT (delivery_status) DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    last_updated = NOW();

INSERT INTO hourly_volume_summary
(order_date, hour_bucket, total_orders)
VALUES

-- Day 1
('2026-05-28', 8, 25),
('2026-05-28', 9, 40),
('2026-05-28',10, 65),
('2026-05-28',11, 95),
('2026-05-28',12,140),
('2026-05-28',13,165),
('2026-05-28',14,120),
('2026-05-28',15,90),
('2026-05-28',16,75),
('2026-05-28',17,110),
('2026-05-28',18,180),
('2026-05-28',19,240),
('2026-05-28',20,290),
('2026-05-28',21,260),
('2026-05-28',22,170),

-- Day 2
('2026-05-29', 8, 30),
('2026-05-29', 9, 55),
('2026-05-29',10, 80),
('2026-05-29',11,120),
('2026-05-29',12,180),
('2026-05-29',13,210),
('2026-05-29',14,170),
('2026-05-29',15,115),
('2026-05-29',16,95),
('2026-05-29',17,140),
('2026-05-29',18,220),
('2026-05-29',19,310),
('2026-05-29',20,360),
('2026-05-29',21,330),
('2026-05-29',22,210),

-- Day 3 (Weekend)
('2026-05-30', 8, 45),
('2026-05-30', 9, 75),
('2026-05-30',10,110),
('2026-05-30',11,170),
('2026-05-30',12,250),
('2026-05-30',13,290),
('2026-05-30',14,240),
('2026-05-30',15,180),
('2026-05-30',16,150),
('2026-05-30',17,210),
('2026-05-30',18,320),
('2026-05-30',19,430),
('2026-05-30',20,510),
('2026-05-30',21,470),
('2026-05-30',22,310);

DO $$
BEGIN
    RAISE NOTICE 'Seed data inserted successfully at %', NOW();
END
$$;

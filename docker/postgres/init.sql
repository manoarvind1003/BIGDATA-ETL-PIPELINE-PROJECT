-- Grant all privileges to the pipeline user
GRANT ALL PRIVILEGES ON DATABASE food_delivery_db TO pipeline_user;

-- Allow connecting
ALTER DATABASE food_delivery_db OWNER TO pipeline_user;

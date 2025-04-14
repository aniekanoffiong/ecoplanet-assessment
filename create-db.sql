CREATE TABLE company (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL
);

CREATE TABLE location (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  company_id INTEGER REFERENCES company(id) ON DELETE SET NULL
);

CREATE TABLE device (
  id SERIAL PRIMARY KEY,
  title VARCHAR NOT NULL,
  location_id INTEGER REFERENCES location(id) ON DELETE SET NULL
);

CREATE TABLE consumption_log (
  id SERIAL PRIMARY KEY,
  device_id INTEGER REFERENCES device(id) ON DELETE SET NULL,
  value NUMERIC(20, 18) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  is_processed BOOLEAN DEFAULT FALSE
);



-- 1. Insert 10 companies
INSERT INTO company (name)
SELECT 'Company ' || gs
FROM generate_series(1, 10) AS gs;

-- 2. Insert 20 locations per company (200 total)
INSERT INTO location (name, company_id)
SELECT 'Location ' || row_number() OVER (), c.id
FROM company c,
     generate_series(1, 20);

-- 3. Insert 20–35 devices per location
WITH device_counts AS (
  SELECT id AS location_id, (20 + floor(random() * 16)::int) AS device_count
  FROM location
),
devices AS (
  SELECT dc.location_id, 'Device ' || row_number() OVER () AS title
  FROM device_counts dc,
       generate_series(1, 35) AS g
  WHERE g <= dc.device_count
)
INSERT INTO device (title, location_id)
SELECT title, location_id
FROM devices;

-- 4. Insert 250–1000 consumption logs per device
DO $$
DECLARE
  device_rec RECORD;
  log_count INT;
BEGIN
  FOR device_rec IN SELECT id FROM device LOOP
    log_count := 250 + floor(random() * 751)::int;

    INSERT INTO consumption_log (device_id, value, timestamp, is_processed)
    SELECT
      device_rec.id,
      round((random() * 0.9999999998 + 0.0000000001)::numeric, 10), -- random float up to 1000
      NOW() - (random() * interval '30 days'), -- random timestamp in past 30 days
      (random() < 0.5) -- 50% chance of being processed
    FROM generate_series(1, log_count);
  END LOOP;
END $$;

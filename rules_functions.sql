
-- Function to add column to broker_points
CREATE OR REPLACE FUNCTION add_column_to_broker_points(column_name text, column_type text)
RETURNS void AS $$
BEGIN
    EXECUTE format('ALTER TABLE broker_points ADD COLUMN IF NOT EXISTS %I %s DEFAULT 0', column_name, column_type);
END;
$$ LANGUAGE plpgsql;

-- Function to drop column from broker_points
CREATE OR REPLACE FUNCTION drop_column_from_broker_points(column_name text)
RETURNS void AS $$
BEGIN
    EXECUTE format('ALTER TABLE broker_points DROP COLUMN IF EXISTS %I', column_name);
END;
$$ LANGUAGE plpgsql;

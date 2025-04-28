-- Create tables table to store table metadata
CREATE TABLE tables (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    current_snapshot_id UUID,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    manifest_list_id UUID
);

-- Create manifest_lists table to store manifest list entries
CREATE TABLE manifest_lists (
    id BIGSERIAL PRIMARY KEY,
    manifest_list_id UUID NOT NULL,
    manifest_file_id UUID NOT NULL
);

-- Create manifest_files table to track data files
CREATE TABLE manifest_files (
    manifest_file_id UUID PRIMARY KEY,
    datafile_location VARCHAR(255) NOT NULL
);


-- -- Add foreign key constraints
-- ALTER TABLE tables
-- ADD CONSTRAINT fk_manifest_list
-- FOREIGN KEY (manifest_list_id) 
-- REFERENCES manifest_lists(manifest_list_id);


-- ALTER TABLE manifest_lists
-- ADD CONSTRAINT fk_manifest_file
-- FOREIGN KEY (manifest_file_id) 
-- REFERENCES manifest_files(manifest_file_id);
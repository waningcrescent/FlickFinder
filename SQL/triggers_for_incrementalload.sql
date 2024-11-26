-- Step 1: Use Netflix database
-- Step 1: Use Netflix database
USE netflix;

-- Drop Netflix triggers if they exist
DROP TRIGGER IF EXISTS after_netflix_insert;
DROP TRIGGER IF EXISTS after_netflix_update;
DROP TRIGGER IF EXISTS after_netflix_delete;

-- Step 2: Use Amazon database
USE amazon;

-- Drop AmazonPrime triggers if they exist
DROP TRIGGER IF EXISTS after_amazon_insert;
DROP TRIGGER IF EXISTS after_amazon_update;
DROP TRIGGER IF EXISTS after_amazon_delete;

-- Step 3: Use Hotstar database
USE hotstar;

-- Drop Hotstar triggers if they exist
DROP TRIGGER IF EXISTS after_hotstar_insert;
DROP TRIGGER IF EXISTS after_hotstar_update;
DROP TRIGGER IF EXISTS after_hotstar_delete;



USE netflix;

-- Step 2: Drop and recreate Netflix table
-- Step 3: Drop and recreate Netflix trigger_log table
DROP TABLE IF EXISTS trigger_log;

CREATE TABLE trigger_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,  -- 'INSERT', 'UPDATE', or 'DELETE'
    row_id VARCHAR(50) NOT NULL,     -- The ID of the affected row
    title VARCHAR(255),              -- Title of the affected row
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed TINYINT(1) DEFAULT 0   -- Flag to track if the log has been processed
);

-- Step 4: Add triggers for Netflix
DELIMITER $$

CREATE TRIGGER after_netflix_insert
AFTER INSERT ON Netflix
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('Netflix', 'INSERT', NEW.show_id, NEW.title);
END$$

CREATE TRIGGER after_netflix_update
AFTER UPDATE ON Netflix
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('Netflix', 'UPDATE', NEW.show_id, NEW.title);
END$$

CREATE TRIGGER after_netflix_delete
AFTER DELETE ON Netflix
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('Netflix', 'DELETE', OLD.show_id, OLD.title);
END$$

DELIMITER ;

-- Step 5: Use Amazon database
USE amazon;

-- Step 6: Drop and recreate AmazonPrime table
-- Step 7: Drop and recreate AmazonPrime trigger_log table
DROP TABLE IF EXISTS trigger_log;

CREATE TABLE trigger_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,  -- 'INSERT', 'UPDATE', or 'DELETE'
    row_id VARCHAR(50) NOT NULL,     -- The ID of the affected row
    title VARCHAR(255),              -- Title of the affected row
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed TINYINT(1) DEFAULT 0   -- Flag to track if the log has been processed
);

-- Step 8: Add triggers for AmazonPrime
DELIMITER $$

CREATE TRIGGER after_amazon_insert
AFTER INSERT ON AmazonPrime
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('AmazonPrime', 'INSERT', NEW.id, NEW.title);
END$$

CREATE TRIGGER after_amazon_update
AFTER UPDATE ON AmazonPrime
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('AmazonPrime', 'UPDATE', NEW.id, NEW.title);
END$$

CREATE TRIGGER after_amazon_delete
AFTER DELETE ON AmazonPrime
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('AmazonPrime', 'DELETE', OLD.id, OLD.title);
END$$

DELIMITER ;

-- Step 9: Use Hotstar database
USE hotstar;

-- Step 10: Drop and recreate Hotstar table
-- Step 11: Drop and recreate Hotstar trigger_log table
DROP TABLE IF EXISTS trigger_log;

CREATE TABLE trigger_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,  -- 'INSERT', 'UPDATE', or 'DELETE'
    row_id VARCHAR(50) NOT NULL,     -- The ID of the affected row
    title VARCHAR(255),              -- Title of the affected row
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed TINYINT(1) DEFAULT 0   -- Flag to track if the log has been processed
);

-- Step 12: Add triggers for Hotstar
DELIMITER $$

CREATE TRIGGER after_hotstar_insert
AFTER INSERT ON Hotstar
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('Hotstar', 'INSERT', NEW.hotstar_id, NEW.title);
END$$

CREATE TRIGGER after_hotstar_update
AFTER UPDATE ON Hotstar
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('Hotstar', 'UPDATE', NEW.hotstar_id, NEW.title);
END$$

CREATE TRIGGER after_hotstar_delete
AFTER DELETE ON Hotstar
FOR EACH ROW
BEGIN
    INSERT INTO trigger_log (table_name, operation, row_id, title)
    VALUES ('Hotstar', 'DELETE', OLD.hotstar_id, OLD.title);
END$$

DELIMITER ;

-- 1. Створюємо довідник країн
CREATE TABLE locations (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- 2. Створюємо довідник рекламодавців
CREATE TABLE advertisers (
    advertiser_id INT AUTO_INCREMENT PRIMARY KEY,
    advertiser_name VARCHAR(100) UNIQUE NOT NULL
);

-- 3. Створюємо таблицю користувачів
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    age INT,
    gender ENUM('Male', 'Female'),
    location_id INT,
    signup_date DATE,
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- 4. Таблиця інтересів (Нормалізація 1NF)
CREATE TABLE user_interests (
    user_id INT,
    interest_name VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 5. Створюємо таблицю кампаній (Нормалізація 3NF)
CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    advertiser_id INT,
    campaign_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    target_age_min INT,
    target_age_max INT,
    target_interest VARCHAR(50),
    target_location_id INT,
    ad_slot_size VARCHAR(50),
    budget DECIMAL(12, 2),
    FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id),
    FOREIGN KEY (target_location_id) REFERENCES locations(location_id)
);

-- 6. Головна таблиця фактів
CREATE TABLE events (
    event_id VARCHAR(36) PRIMARY KEY,
    campaign_id INT,
    user_id INT,
    device ENUM('Mobile', 'Desktop', 'Tablet'),
    location_id INT,
    event_timestamp DATETIME,
    bid_amount DECIMAL(12, 2),
    ad_cost DECIMAL(12, 2),
    was_clicked BOOLEAN,
    click_timestamp DATETIME,
    ad_revenue DECIMAL(12, 2),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- дозаливаэмо колонки
ALTER TABLE campaigns ADD COLUMN remaining_budget FLOAT;
ALTER TABLE campaigns ADD COLUMN ad_slot_size VARCHAR(50);
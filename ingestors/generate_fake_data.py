import mysql.connector
from faker import Faker
import random

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="dari_db"
)
cursor = conn.cursor()

# Create table with new schema
cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255),
        description TEXT,
        price DECIMAL(10, 2),
        latitude DOUBLE,
        longitude DOUBLE,
        type ENUM('House', 'Villa', 'Apartment', 'Studio'),
        rooms ENUM('S0', 'S1', 'S2', 'S3', 'S4', 'S5', 'Any')
    )
""")

# Generate fake data
fake = Faker()
num_records = 50000

types = ['House', 'Villa', 'Apartment', 'Studio']
room_options = ['S0', 'S1', 'S2', 'S3', 'S4', 'S5', 'Any']

for _ in range(num_records):
    title = fake.sentence(nb_words=4)
    description = fake.paragraph(nb_sentences=5)
    price = round(random.uniform(100000, 500000), 2)
    latitude = fake.latitude()
    longitude = fake.longitude()
    property_type = random.choice(types)
    rooms = random.choice(room_options)

    cursor.execute("""
        INSERT INTO listings (title, description, price, latitude, longitude, type, rooms)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (title, description, price, latitude, longitude, property_type, rooms))

conn.commit()
print(f"✅ Inserted {num_records} fake listings into 'listings' table.")

cursor.close()
conn.close()

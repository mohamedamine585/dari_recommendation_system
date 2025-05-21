import pymysql
from faker import Faker
import random

def get_positive_input(prompt, default=1):
    """Get positive integer input from user with validation"""
    while True:
        try:
            user_input = int(input(f"{prompt} [{default}]: ") or default)
            if user_input < 1:
                print("Please enter a number greater than 0")
                continue
            return user_input
        except ValueError:
            print("Please enter a valid number")

# Get user input with validation
print("=== Database Population Settings ===")
num_users = get_positive_input("Enter number of users to create", 10)
max_annonces_per_user = get_positive_input("Enter maximum annonces per user", 3)
max_attachments_per_annonce = get_positive_input("Enter maximum attachments per annonce", 3)
# max_searches_per_user = get_positive_input("Enter maximum searches per user", 2)  # DISABLED
max_interactions_per_user = get_positive_input("Enter number of interactions per user", 5)

faker = Faker()

# Connect to MySQL
try:
    conn = pymysql.connect(
        host='localhost',
        user='admin',
        password='admin',
        db='dari_db',
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    print("\n=== Resetting Database ===")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("SHOW TABLES")
    all_tables = [table[0] for table in cursor.fetchall()]
    for table in all_tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped table {table}")
        except Exception as e:
            print(f"Error dropping table {table}: {e}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    print("\n=== Creating Tables ===")
    cursor.execute("""
    CREATE TABLE user (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        telephone VARCHAR(255),
        nom VARCHAR(255),
        active BOOLEAN DEFAULT TRUE,
        abonnement_id BIGINT
    )
    """)
    cursor.execute("""
    CREATE TABLE user_roles (
        user_id BIGINT,
        roles VARCHAR(255),
        FOREIGN KEY (user_id) REFERENCES user(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE annonce (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        titre VARCHAR(100) NOT NULL,
        description TEXT,
        prix FLOAT NOT NULL,
        lease_duration VARCHAR(50),
        type VARCHAR(50),
        rooms VARCHAR(50),
        latitude DOUBLE,
        longitude DOUBLE,
        type_bien VARCHAR(50),
        status VARCHAR(50),
        user_id BIGINT,
        FOREIGN KEY (user_id) REFERENCES user(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE annonce_attachment_paths (
        annonce_id BIGINT,
        attachment_paths VARCHAR(255),
        FOREIGN KEY (annonce_id) REFERENCES annonce(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE usearch_query (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        query TEXT,
        min_prix FLOAT,
        max_prix FLOAT,
        type INT,
        status_annonce INT,
        latitude DOUBLE,
        longitude DOUBLE,
        radius DOUBLE,
        rooms INT,
        type_bien INT,
        user_id BIGINT,
        FOREIGN KEY (user_id) REFERENCES user(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE user_interaction (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        annonce_id BIGINT NOT NULL,
        interaction_score FLOAT NOT NULL,
        interaction_type ENUM('VIEW', 'SAVE', 'CONTACT', 'RATING') NOT NULL,
        interaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES user(id),
        FOREIGN KEY (annonce_id) REFERENCES annonce(id)
    )
    """)

    print(f"\n=== Inserting {num_users} Users ===")
    user_ids = []
    roles = ['ROLE_USER', 'ROLE_ADMIN']
    for i in range(num_users):
        username = faker.unique.user_name()
        password = 'password123'
        telephone = faker.phone_number()
        nom = faker.name()
        active = 1
        cursor.execute("""
            INSERT INTO user (username, password, telephone, nom, active)
            VALUES (%s, %s, %s, %s, %s)
        """, (username, password, telephone, nom, active))
        user_id = cursor.lastrowid
        user_ids.append(user_id)

        user_roles = random.sample(roles, random.randint(1, 2))
        for role in user_roles:
            cursor.execute("""
                INSERT INTO user_roles (user_id, roles)
                VALUES (%s, %s)
            """, (user_id, role))
        print(f"Created user {i+1}/{num_users}: {username} with roles {', '.join(user_roles)}")

    TYPE_ANNONCE = ['VENTE', 'LOCATION']
    STATUS_ANNONCE = ['ACTIVE', 'INACTIVE', 'EN_ATTENTE']
    TYPE_BIEN = ['ANY', 'APARTMENT', 'HOUSE', 'VILLA', 'STUDIO', 'CONDO', 'TOWNHOUSE', 'PENTHOUSE', 'DUPLEX', 'LOFT', 'BUNGALOW', 'FARMHOUSE', 'COTTAGE']
    ROOMS = ['ANY', 'S1', 'S2', 'S3', 'S4', 'S5']
    LEASE_DURATIONS = ['ANY', 'DAY', 'WEEK', 'MONTH', 'SEMESTER', 'QUARTER', 'YEAR', 'FLEXIBLE']


    print(f"\n=== Inserting Annonces ===")
    annonce_ids = []
    for user_id in user_ids:
        for _ in range(random.randint(1, max_annonces_per_user)):
            titre = faker.sentence(nb_words=6)[:100]
            description = faker.text()
            prix = round(random.uniform(10000, 300000), 2)
            lease_duration = random.choice(LEASE_DURATIONS)
            ann_type = random.choice(TYPE_ANNONCE)
            room = random.choice(ROOMS)
            lat, lng = float(faker.latitude()), float(faker.longitude())
            type_b = random.choice(TYPE_BIEN)
            status = random.choice(STATUS_ANNONCE)

            cursor.execute("""
                INSERT INTO annonce 
                (titre, description, prix, lease_duration, type, rooms, latitude, longitude, type_bien, status, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (titre, description, prix, lease_duration, ann_type, room, lat, lng, type_b, status, user_id))
            annonce_id = cursor.lastrowid
            annonce_ids.append(annonce_id)

            for _ in range(random.randint(1, max_attachments_per_annonce)):
                path = f"/uploads/{faker.uuid4()}.jpg"
                cursor.execute("""
                    INSERT INTO annonce_attachment_paths (annonce_id, attachment_paths)
                    VALUES (%s, %s)
                """, (annonce_id, path))

    # DISABLED: Insert search queries (can be re-enabled later)
    # print(f"\n=== Inserting Search Queries ===")
    # TYPE_ANNONCE_INT = {'VENTE': 0, 'LOCATION': 1, 'ECHANGE': 2}
    # STATUS_ANNONCE_INT = {'ACTIVE': 0, 'INACTIVE': 1, 'EN_ATTENTE': 2}
    # TYPE_BIEN_INT = {
    #     'ANY': 0, 'APARTMENT': 1, 'HOUSE': 2, 'VILLA': 3, 'STUDIO': 4,
    #     'CONDO': 5, 'TOWNHOUSE': 6, 'PENTHOUSE': 7, 'DUPLEX': 8,
    #     'LOFT': 9, 'BUNGALOW': 10, 'FARMHOUSE': 11, 'COTTAGE': 12
    # }
    # for user_id in user_ids:
    #     for _ in range(random.randint(1, max_searches_per_user)):
    #         query = faker.sentence()
    #         min_prix = round(random.uniform(5000, 15000), 2)
    #         max_prix = round(random.uniform(min_prix + 1000, 30000), 2)
    #         ann_type = random.choice(list(TYPE_ANNONCE_INT.values()))
    #         status = random.choice(list(STATUS_ANNONCE_INT.values()))
    #         lat = float(faker.latitude())
    #         lng = float(faker.longitude())
    #         radius = round(random.uniform(1.0, 50.0), 2)
    #         room = random.randint(1, 5)
    #         type_b = random.choice(list(TYPE_BIEN_INT.values()))
    #         cursor.execute("""
    #             INSERT INTO usearch_query 
    #             (query, min_prix, max_prix, type, status_annonce, latitude, longitude, radius, rooms, type_bien, user_id)
    #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #         """, (query, min_prix, max_prix, ann_type, status, lat, lng, radius, room, type_b, user_id))

    print("\n=== Generating User Interactions ===")
    interaction_types = ['VIEW', 'SAVE', 'CONTACT', 'RATING']
    for user_id in user_ids:
        for _ in range(max_interactions_per_user):
            print(f"Generating interactions for user {user_id}")
            annonce_id = random.choice(annonce_ids)
            interaction_type = random.choice(interaction_types)
            interaction_score = round(random.uniform(0.1, 1.0), 2)
            cursor.execute("""
                INSERT INTO user_interaction 
                (user_id, annonce_id, interaction_score, interaction_type)
                VALUES (%s, %s, %s, %s)
            """, (user_id, annonce_id, interaction_score, interaction_type))

    conn.commit()
    print("\n=== Database Population Complete ===")

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        conn.close()

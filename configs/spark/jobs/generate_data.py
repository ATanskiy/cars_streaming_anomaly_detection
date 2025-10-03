import random
from ...constants import TABLE_CAR_MODELS_COLUMNS, TABLE_CAR_COLORS_COLUMNS, \
      TABLE_CARS_COLUMNS

def generate_car_models_data(spark):

    data = [
        (1, 'Mazda', '3'),
        (2, 'Mazda', '6'),
        (3, 'Toyota', 'Corolla'),
        (4, 'Hyundai', 'i20'),
        (5, 'Kia', 'Sportage'),
        (6, 'Kia', 'Rio'),
        (7, 'Kia', 'Picanto')
    ]

    df = spark.createDataFrame(data, schema=TABLE_CAR_MODELS_COLUMNS)
    return df


def generate_car_colors_data(spark):

    data = [
        (1, 'Black'),
        (2, 'Red'),
        (3, 'Gray'),
        (4, 'White'),
        (5, 'Green'),
        (6, 'Blue'),
        (7, 'Pink')
    ]
    df = spark.createDataFrame(data, schema=TABLE_CAR_COLORS_COLUMNS)
    return df

def generate_cars_table_data(spark):

    # Generate 20 unique car IDs
    car_ids = random.sample(range(1000000, 9999999), 20)

    # Generate data
    data = []
    for car_id in car_ids:
        driver_id = random.randint(100000000, 999999999)  # 9 digits
        model_id = random.randint(1, 7)  # 1 to 7
        color_id = random.randint(1, 7)  # 1 to 7
        data.append((car_id, driver_id, model_id, color_id))

    # Create DataFrame
    df = spark.createDataFrame(data, schema=TABLE_CARS_COLUMNS)

    return df
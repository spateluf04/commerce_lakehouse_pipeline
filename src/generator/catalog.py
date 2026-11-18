"""Reference data the generator samples from."""

FIRST_NAMES = (
    "Ava", "Liam", "Olivia", "Noah", "Emma", "Mateo", "Sophia", "Elijah", "Isabella", "Lucas",
    "Mia", "Ethan", "Amelia", "Aiden", "Harper", "James", "Priya", "Wei", "Fatima", "Diego",
    "Chloe", "Kenji", "Zara", "Omar", "Grace", "Leo", "Nina", "Samuel", "Aisha", "Daniel",
)
LAST_NAMES = (
    "Smith", "Johnson", "Garcia", "Brown", "Lee", "Martinez", "Davis", "Lopez", "Wilson", "Nguyen",
    "Patel", "Kim", "Anderson", "Thomas", "Hernandez", "Moore", "Clark", "Lewis", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Hill", "Green", "Adams", "Baker", "Chen",
)
LOCATIONS = (
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"), ("Phoenix", "AZ"),
    ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"), ("Austin", "TX"),
    ("Orlando", "FL"), ("Seattle", "WA"), ("Denver", "CO"), ("Boston", "MA"), ("Nashville", "TN"),
    ("Portland", "OR"), ("Atlanta", "GA"), ("Miami", "FL"), ("Minneapolis", "MN"), ("Charlotte", "NC"),
)

# category -> (product nouns, (min list price, max list price))
CATEGORIES = {
    "Electronics": (("Headphones", "Charger", "Speaker", "Keyboard", "Mouse", "Monitor", "Webcam", "Smartwatch"), (15.0, 400.0)),
    "Apparel": (("T-Shirt", "Hoodie", "Jacket", "Cap", "Socks", "Jeans", "Sneakers"), (8.0, 120.0)),
    "Home & Kitchen": (("Mug", "Blender", "Pan", "Lamp", "Towel Set", "Knife Set", "Cutting Board"), (6.0, 180.0)),
    "Sports & Outdoors": (("Water Bottle", "Yoga Mat", "Backpack", "Tent", "Dumbbell Set", "Bike Light"), (10.0, 250.0)),
    "Books": (("Novel", "Cookbook", "Biography", "Graphic Novel", "Travel Guide"), (5.0, 45.0)),
    "Toys & Games": (("Puzzle", "Board Game", "Action Figure", "Building Set", "Plush Toy"), (7.0, 90.0)),
    "Beauty": (("Moisturizer", "Shampoo", "Lip Balm", "Sunscreen", "Face Mask"), (4.0, 60.0)),
}
ADJECTIVES = ("Classic", "Premium", "Everyday", "Compact", "Deluxe", "Eco", "Pro", "Essential", "Ultra", "Vintage")

# (value, relative weight)
ORDER_STATUSES = (("placed", 15), ("shipped", 25), ("delivered", 50), ("cancelled", 6), ("returned", 4))
PAYMENT_METHODS = (("credit_card", 55), ("debit_card", 20), ("paypal", 15), ("gift_card", 5), ("digital_wallet", 5))
DISCOUNTS = (0.0, 0.0, 0.0, 0.0, 0.10, 0.15, 0.20)  # most line items sell at list price

# Relative order volume for each hour of the day (UTC), from 00:00 to 23:00: quiet overnight, peaks at lunch and in the evening.
HOUR_WEIGHTS = (1, 1, 1, 1, 1, 2, 3, 5, 6, 7, 8, 9, 10, 9, 8, 8, 9, 10, 12, 13, 12, 9, 5, 3)

"""
Synthetic data generator for e-commerce datasets.

This module provides the SyntheticDataGenerator class for generating realistic
e-commerce data including customers, products, and orders with proper distributions.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Optional

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    """
    Generate synthetic e-commerce data including customers, products, and orders.

    This class uses realistic distributions to generate data:
    - Customer ages follow a normal distribution around 35 years
    - Customer orders follow a Pareto distribution (20% make 80% of orders)
    - Prices range from 10 to 500
    - Ratings range from 1 to 5
    """

    # Product categories
    CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books"]

    # Order statuses
    ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]

    # Payment methods
    PAYMENT_METHODS = ["Credit Card", "PayPal", "Bank Transfer", "Cash on Delivery"]

    def __init__(
        self,
        num_customers: int = 100000,
        num_products: int = 10000,
        num_orders: int = 1000000,
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize the SyntheticDataGenerator.

        Args:
            num_customers (int): Number of customers to generate. Default: 100,000.
            num_products (int): Number of products to generate. Default: 10,000.
            num_orders (int): Number of orders to generate. Default: 1,000,000.
            seed (Optional[int]): Random seed for reproducibility. Default: None.
        """
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_orders = num_orders
        self.seed = seed

        # Set random seeds for reproducibility
        if seed is not None:
            np.random.seed(seed)
            Faker.seed(seed)

        self.fake = Faker()

        logger.info(
            f"Initialized SyntheticDataGenerator: "
            f"customers={num_customers}, products={num_products}, orders={num_orders}"
        )

    def generate_customers(self) -> pd.DataFrame:
        """
        Generate synthetic customer data.

        Returns a DataFrame with the following columns:
        - customer_id: Unique customer identifier
        - name: Customer full name
        - email: Customer email address
        - age: Customer age (normal distribution, mean=35, std=15)
        - city: Customer city
        - country: Customer country
        - registration_date: Date customer registered (within last 3 years)

        Returns:
            pd.DataFrame: Generated customer data.
        """
        logger.info(f"Generating {self.num_customers} customers...")

        data = {
            "customer_id": np.arange(1, self.num_customers + 1),
            "name": [self.fake.name() for _ in tqdm(range(self.num_customers), desc="Generating customer names")],
            "email": [self.fake.email() for _ in tqdm(range(self.num_customers), desc="Generating emails")],
            "age": np.clip(
                np.random.normal(loc=35, scale=15, size=self.num_customers).astype(int),
                min=18,
                max=80,
            ),
            "city": [self.fake.city() for _ in tqdm(range(self.num_customers), desc="Generating cities")],
            "country": [self.fake.country() for _ in tqdm(range(self.num_customers), desc="Generating countries")],
            "registration_date": [
                (datetime.now() - timedelta(days=int(np.random.uniform(0, 1095)))).date()
                for _ in tqdm(range(self.num_customers), desc="Generating registration dates")
            ],
        }

        customers_df = pd.DataFrame(data)
        logger.info(f"Generated {len(customers_df)} customers.")
        return customers_df

    def generate_products(self) -> pd.DataFrame:
        """
        Generate synthetic product data.

        Returns a DataFrame with the following columns:
        - product_id: Unique product identifier
        - name: Product name
        - category: Product category (Electronics, Clothing, Home, Sports, Books)
        - price: Product price (uniformly distributed between 10 and 500)
        - stock: Current stock quantity (uniformly distributed between 0 and 1000)
        - rating: Product rating (uniformly distributed between 1 and 5, rounded to 1 decimal)

        Returns:
            pd.DataFrame: Generated product data.
        """
        logger.info(f"Generating {self.num_products} products...")

        data = {
            "product_id": np.arange(1, self.num_products + 1),
            "name": [self.fake.sentence(nb_words=3)[:-1] for _ in tqdm(range(self.num_products), desc="Generating product names")],
            "category": np.random.choice(
                self.CATEGORIES, size=self.num_products
            ).tolist(),
            "price": np.round(
                np.random.uniform(10, 500, self.num_products), 2
            ).tolist(),
            "stock": np.random.randint(0, 1001, self.num_products).tolist(),
            "rating": np.round(
                np.random.uniform(1, 5, self.num_products), 1
            ).tolist(),
        }

        products_df = pd.DataFrame(data)
        logger.info(f"Generated {len(products_df)} products.")
        return products_df

    def generate_orders(
        self,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Generate synthetic order data with Pareto distribution.

        Orders follow a Pareto distribution where 20% of customers make 80% of orders.
        This is achieved by assigning order quantities to customers according to the
        Pareto principle.

        Args:
            customers_df (pd.DataFrame): Generated customers DataFrame.
            products_df (pd.DataFrame): Generated products DataFrame.

        Returns a DataFrame with the following columns:
        - order_id: Unique order identifier
        - customer_id: Reference to customer
        - product_id: Reference to product
        - quantity: Order quantity (uniformly distributed between 1 and 10)
        - order_date: Date of the order (within last 1 year)
        - order_status: Status of the order
        - payment_method: Payment method used

        Returns:
            pd.DataFrame: Generated order data.
        """
        logger.info(f"Generating {self.num_orders} orders with Pareto distribution...")

        customer_ids = customers_df["customer_id"].values
        product_ids = products_df["product_id"].values

        # Generate Pareto-distributed customer order frequencies
        # This ensures 20% of customers make 80% of orders
        logger.info("Applying Pareto distribution to customers...")
        pareto_distribution = np.random.pareto(a=1.16, size=self.num_customers)
        # Normalize to sum to num_orders
        pareto_distribution = (
            pareto_distribution / pareto_distribution.sum() * self.num_orders
        ).astype(int)

        # Generate orders based on Pareto distribution
        logger.info("Generating order data...")
        customer_id_list = []
        product_id_list = []
        quantity_list = []
        order_date_list = []
        order_status_list = []
        payment_method_list = []

        order_count = 0
        for customer_idx, num_customer_orders in enumerate(tqdm(pareto_distribution, desc="Generating orders")):
            for _ in range(num_customer_orders):
                if order_count >= self.num_orders:
                    break

                customer_id_list.append(customer_ids[customer_idx])
                product_id_list.append(np.random.choice(product_ids))
                quantity_list.append(np.random.randint(1, 11))
                
                # Generate random date within last 1 year
                days_ago = np.random.randint(0, 365)
                order_date_list.append(
                    (datetime.now() - timedelta(days=days_ago)).date()
                )
                
                order_status_list.append(np.random.choice(self.ORDER_STATUSES))
                payment_method_list.append(np.random.choice(self.PAYMENT_METHODS))

                order_count += 1

            if order_count >= self.num_orders:
                break

        # Trim to exact num_orders if necessary
        if len(customer_id_list) > self.num_orders:
            customer_id_list = customer_id_list[: self.num_orders]
            product_id_list = product_id_list[: self.num_orders]
            quantity_list = quantity_list[: self.num_orders]
            order_date_list = order_date_list[: self.num_orders]
            order_status_list = order_status_list[: self.num_orders]
            payment_method_list = payment_method_list[: self.num_orders]

        data = {
            "order_id": np.arange(1, len(customer_id_list) + 1),
            "customer_id": customer_id_list,
            "product_id": product_id_list,
            "quantity": quantity_list,
            "order_date": order_date_list,
            "order_status": order_status_list,
            "payment_method": payment_method_list,
        }

        orders_df = pd.DataFrame(data)
        logger.info(f"Generated {len(orders_df)} orders.")
        return orders_df

    def generate_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Generate all datasets (customers, products, and orders).

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Tuple containing
                (customers_df, products_df, orders_df)
        """
        logger.info("Starting complete data generation...")
        
        customers_df = self.generate_customers()
        products_df = self.generate_products()
        orders_df = self.generate_orders(customers_df, products_df)

        logger.info("Data generation completed successfully!")
        return customers_df, products_df, orders_df

    @staticmethod
    def save_to_csv(
        df: pd.DataFrame,
        filepath: Path,
        index: bool = False,
    ) -> None:
        """
        Save a DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): DataFrame to save.
            filepath (Path): Path to save the CSV file.
            index (bool): Whether to include the index. Default: False.
        """
        df.to_csv(filepath, index=index)
        logger.info(f"Saved {len(df)} rows to {filepath}")

    @staticmethod
    def save_to_parquet(
        df: pd.DataFrame,
        filepath: Path,
        index: bool = False,
    ) -> None:
        """
        Save a DataFrame to a Parquet file (more efficient than CSV for large datasets).

        Args:
            df (pd.DataFrame): DataFrame to save.
            filepath (Path): Path to save the Parquet file.
            index (bool): Whether to include the index. Default: False.
        """
        df.to_parquet(filepath, index=index)
        logger.info(f"Saved {len(df)} rows to {filepath}")


def main() -> None:
    """
    Main entry point for data generation.
    Generates default datasets and saves them to data/raw/ directory.
    """
    from config import DATA_RAW_DIR

    logger.info("=" * 70)
    logger.info("E-commerce Synthetic Data Generator")
    logger.info("=" * 70)

    # Create generator with default parameters
    generator = SyntheticDataGenerator(
        num_customers=100000,
        num_products=10000,
        num_orders=1000000,
        seed=42,  # Set seed for reproducibility
    )

    # Generate all datasets
    customers_df, products_df, orders_df = generator.generate_all()

    # Display summary statistics
    logger.info("\n" + "=" * 70)
    logger.info("Dataset Summary Statistics")
    logger.info("=" * 70)
    logger.info(f"Customers: {len(customers_df):,}")
    logger.info(f"  - Age range: {customers_df['age'].min()}-{customers_df['age'].max()} years")
    logger.info(f"  - Mean age: {customers_df['age'].mean():.1f} years")
    logger.info(f"\nProducts: {len(products_df):,}")
    logger.info(f"  - Price range: ${products_df['price'].min():.2f}-${products_df['price'].max():.2f}")
    logger.info(f"  - Mean price: ${products_df['price'].mean():.2f}")
    logger.info(f"  - Average stock: {products_df['stock'].mean():.1f} units")
    logger.info(f"  - Average rating: {products_df['rating'].mean():.2f}/5.0")
    logger.info(f"\nOrders: {len(orders_df):,}")
    logger.info(f"  - Average quantity per order: {orders_df['quantity'].mean():.2f}")
    logger.info(f"  - Order status distribution:\n{orders_df['order_status'].value_counts().to_string()}")
    logger.info("\n" + "=" * 70)

    # Save to CSV
    logger.info("Saving data to CSV format...")
    generator.save_to_csv(customers_df, DATA_RAW_DIR / "customers.csv")
    generator.save_to_csv(products_df, DATA_RAW_DIR / "products.csv")
    generator.save_to_csv(orders_df, DATA_RAW_DIR / "orders.csv")

    # Also save to Parquet for better performance with large datasets
    logger.info("Saving data to Parquet format (optimized for large datasets)...")
    generator.save_to_parquet(customers_df, DATA_RAW_DIR / "customers.parquet")
    generator.save_to_parquet(products_df, DATA_RAW_DIR / "products.parquet")
    generator.save_to_parquet(orders_df, DATA_RAW_DIR / "orders.parquet")

    logger.info("\n" + "=" * 70)
    logger.info("All data generation completed!")
    logger.info(f"Data saved to: {DATA_RAW_DIR}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
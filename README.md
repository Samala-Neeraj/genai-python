# E-commerce Data Pipeline Project

This project builds an e-commerce data pipeline in Python for generating fake customer, product, and order data, and analyzing it using PySpark to uncover business insights.

## Project Structure

- `src/`: Contains all Python source code files
- `data/raw/`: Stores generated fake data files
- `data/processed/`: Stores analyzed results and processed data
- `tests/`: Contains test scripts for the project
- `notebooks/`: Jupyter notebooks for exploratory data analysis

## Features

- **Data Generation**: Generate synthetic e-commerce data including customers, products, and orders
- **Data Analysis**: Use PySpark for distributed data processing and business insights
- **Logging**: Comprehensive logging for debugging and monitoring
- **Type Hints**: All functions include type annotations
- **Documentation**: Detailed docstrings for all functions

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Generate data:
   ```bash
   python src/data_generator.py
   ```

3. Run analysis:
   ```bash
   python src/spark_analytics.py
   ```

## Usage

- Configure settings in `src/config.py`
- Run data generation to create fake datasets
- Execute PySpark analytics to generate insights
- Use notebooks for interactive exploration

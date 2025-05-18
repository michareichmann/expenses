# Expenses Tracker

This repository is a simple Python application for tracking monthly expenses.

## Installation

To install the application, follow these steps:

1. Clone the repository: `git clone https://github.com/your-username/expenses.git`
2. Navigate to the project directory: `cd expenses`
3. Install the required packages: `pip install -r requirements.txt`

## Usage

To use the application, follow these steps:

1. Run the main script: `python main.py`
2. The application will prompt you to enter your monthly expenses.
3. After entering all your expenses for a month, the application will calculate and display the total expenses for that month.

## List monthly expenses

To list monthly expenses, you can use the `Data` class provided in the `src.data` module. Here's an example of how to list monthly expenses:

```python
from src.data import Data

# Load the data from the database
data = Data()
expenses = data.load()

# Group the expenses by month
grouped_expenses = expenses.groupby(expenses['Date'].dt.to_period('M'))

# Print the monthly expenses
for month, expenses_in_month in grouped_expenses:
    total_expenses = expenses_in_month['Amount'].sum()
    print(f"Monthly expenses for {month}: {total_expenses}")# List monthly expenses
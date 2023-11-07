# ELT Data Pipeline with dbt

## Overview

This project demonstrates the implementation of an ELT (Extract, Load, Transform) data pipeline using dbt (Data Build Tool) to process, analyze, and present insights from Airbnb and Census data. With dbt, you can easily create and maintain data models, ensuring clean, accurate, and well-documented data for your analysis and reporting.

## Getting Started

Before running dbt commands, make sure you have set up the necessary configurations and dependencies.

### Prerequisites

- [dbt](https://docs.getdbt.com/docs/introduction)
- Your data warehouse - Postgres on Dbeaver
- A version control system (Git)

### Project Structure

- `models/`: This directory contains dbt models that define the transformations for your data.
- `analysis/`: Analytical queries and reports.
- `tests/`: Data tests to ensure data quality.
- `macros/`: Reusable SQL code and dbt macros.
- `data/`: Raw data files.

## Running dbt

To update the data and execute dbt transformations, follow these steps:

### Step 1: Snapshot Your Data

Before running transformations, create snapshots of your data using dbt. This captures the current state of your data for trend analysis. Use the following dbt command:

```bash
dbt snapshot

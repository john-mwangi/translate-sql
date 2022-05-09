# %% [markdown]
# # Objective
# Translate Python code to SQL - preferably a pandas equivalent. Primarily we want to be able to change data wrangling code conducted in Python into an SQL statement. Preferably, this is a convertion of pandas task.
# 
# Options:
# - SQLAlchemy: https://github.com/sqlalchemy
# - Suiba: https://github.com/machow/siuba
# - Blaze: https://github.com/blaze/blaze
# - Ibis: https://github.com/ibis-project/ibis
# 
# References:
# - https://docs.sqlalchemy.org/en/14/core/tutorial.html#ordering-grouping-limiting-offset-ing

# %% [markdown]
# # Import packages

# %%
import pandas as pd
from sqlalchemy import select, create_engine, MetaData, Table, column, func

# %% [markdown]
# # Data importation

# %%
loans_raw = pd.read_csv('./loans_raw.csv', low_memory=False, index_col=0)
repayments_raw = pd.read_csv("./repayments_raw.csv", low_memory=False, index_col=0)

# %%
db_string = "postgresql://postgres:postgres@localhost:5432/postgres"
loans_raw.to_sql(name="loans", con=db_string, if_exists="replace", index=False)
repayments_raw.to_sql(name="repayments", con=db_string, if_exists="replace", index=False)

# %% [markdown]
# # Code to translate

# %%
loan_cols = [
    'loanId',
    'loanAmount',
    'loanType',
    'lateFees',
    'interestSavings',
    'addStatementFee',
    'disbursedOverpaidAmount',
    'repaymentDate',
    'status',
    'disbursementDate',
    'duration',
    'interestRate',
    'customerId',]

repayment_cols = ['scheduleOrder','totalPaymentWithinSchedule', 'repaidDate','loanId', 'status']

# %%
loans_fn = loans_raw[loan_cols]
repayments_fn = repayments_raw[repayment_cols]

# %%
# Code to translate
repayments_proc = pd.merge(left=loans_fn, right=repayments_fn, on="loanId")\
    .sort_values(by=["loanId", "scheduleOrder"]) \
    .groupby(by="loanId", as_index=False) \
        .aggregate(
            repaidDate=("repaidDate", "max"),
            totalPaymentWithinSchedule=("totalPaymentWithinSchedule", "sum"),
            loanAmount = ("loanAmount", "first"),
            scheduleOrder = ("scheduleOrder", "last"),
            status = ("status_y", "last"),
    )

repayments_proc

# %% [markdown]
# # Option 1: SQLAlchemy
# We're starting with this because it is the most well-know solution and most mature solution among the options. We will use `select` to execute select statements on our database.

# %% [markdown]
# ## Database connection

# %%
engine = create_engine(url=db_string)
metadata = MetaData()

# %%
with engine.connect() as conn:
    loans_tbl = Table("loans", metadata, autoload=True, autoload_with=engine)
    repayments_tbl = Table("repayments", metadata, autoload=True, autoload_with=engine)

# %% [markdown]
# ## Selecting specified columns
# Passing a list of columns to be extracted from the database. These queries are executed lazily.

# %%
loan_db_cols = map(lambda x: column(x), loan_cols)

select_stmt = select(from_obj=loans_tbl, columns=loan_db_cols)
print(select_stmt)

# %% [markdown]
# ## Left join

# %%
join_stmt = select(loans_tbl, repayments_tbl).join(repayments_tbl, loans_tbl.c.loanId == repayments_tbl.c.loanId)
print(join_stmt)

# %%
loans_tbl.join?

# %% [markdown]
# ## Order by

# %%
orderby_stmt = select(loans_tbl, repayments_tbl) \
    .join(repayments_tbl, loans_tbl.c.loanId == repayments_tbl.c.loanId) \
    .order_by(loans_tbl.c.loanId, repayments_tbl.c.scheduleOrder)
    
print(orderby_stmt)

# %% [markdown]
# ## Group by

# %%
groupby_stmt = select(loans_tbl, repayments_tbl) \
    .join(repayments_tbl, loans_tbl.c.loanId == repayments_tbl.c.loanId) \
    .order_by(loans_tbl.c.loanId, repayments_tbl.c.scheduleOrder) \
    .group_by(loans_tbl.c.loanId)
    
print(groupby_stmt)

# %% [markdown]
# ## Aggregate functions

# %%
agg_stmt = select(loans_tbl, repayments_tbl, \
    func.max(repayments_tbl.c.repaidDate), \
    func.sum(repayments_tbl.c.totalPaymentWithinSchedule)) \
    .join(repayments_tbl, loans_tbl.c.loanId == repayments_tbl.c.loanId) \
    .order_by(loans_tbl.c.loanId, repayments_tbl.c.scheduleOrder) \
    .group_by(loans_tbl.c.loanId)
    
print(agg_stmt)

# %% [markdown]
# ## Window functions




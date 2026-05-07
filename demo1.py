import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import IntegerType

def main(session: snowpark.Session):
    session.query_tag = "func-gen"

    add_one = sproc(
        lambda session, x: session.sql(f"select {x} + 1").collect()[0][0],
        input_types=[IntegerType()],
        return_type=IntegerType(),
        packages=["snowflake-snowpark-python"],
        session=session,
    )

    ret = add_one(1)

    # Wrap the result in a DataFrame so the worksheet can display it as a table
    return session.create_dataframe([[ret]], schema=["RESULT"])
==========================================================================================================
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import IntegerType

def main(session: snowpark.Session):
    session.query_tag = "func-gen"

    add_two = sproc(
        lambda session, x: session.sql(f"select {x} + 2").collect()[0][0],
        name="add_two_proc",
        replace=True,
        input_types=[IntegerType()],
        return_type=IntegerType(),
        packages=["snowflake-snowpark-python"],
        session=session,
    )

    ret = session.call("add_two_proc", 1)

    # Return as a DataFrame so the Worksheet can render it
    return session.create_dataframe([[ret]], schema=["ADD_TWO_RESULT"])

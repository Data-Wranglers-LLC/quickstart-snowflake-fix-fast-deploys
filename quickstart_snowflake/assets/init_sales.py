import pandas as pd
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas

from dagster import asset
import datetime


@asset(group_name="init_sales", compute_kind="Execute_Procedure")
def SET_FMC_MTD_FL_INIT_SLS(context, snowflake: SnowflakeResource) -> None:
    # Procedure arguments
    dag_name = 'INIT_SALES'
    load_cycle_id = '100'
    load_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get current timestamp and format it

    # Construct the query with string formatting
    query = f"""        CALL SPEEDSHOP_PROC.SET_FMC_MTD_FL_INIT_SLS('{dag_name}', '{load_cycle_id}', '{load_timestamp}');
    """

    # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""        CALL SPEEDSHOP_PROC.EXT_SLS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_BICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""        CALL SPEEDSHOP_PROC.EXT_SLS_BICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_ELECTRICBICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_ELECTRICBICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_INVOICELINEITEMS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_INVOICELINEITEMS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_INVOICES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_INVOICES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_MOTORCYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_MOTORCYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_PASSENGERCARS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_PASSENGERCARS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_PERSONS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_PERSONS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_PURCHASEDBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_PURCHASEDBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_PURCHASEDEBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_PURCHASEDEBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_PURCHASEDMCYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_PURCHASEDMCYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_PURCHASEDPASSCAR_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_PURCHASEDPASSCAR_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["SET_FMC_MTD_FL_INIT_SLS"])
def EXT_SLS_SHOPS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.EXT_SLS_SHOPS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_ADDRESSES_INIT"])
def STG_SLS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_BICYCLES_INIT"])
def STG_SLS_BICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_BICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_ELECTRICBICYCLES_INIT"])
def STG_SLS_ELECTRICBICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_ELECTRICBICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_INVOICELINEITEMS_INIT"])
def STG_SLS_INVOICELINEITEMS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_INVOICELINEITEMS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_INVOICES_INIT"])
def STG_SLS_INVOICES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_INVOICES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_MOTORCYCLES_INIT"])
def STG_SLS_MOTORCYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_MOTORCYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_PASSENGERCARS_INIT"])
def STG_SLS_PASSENGERCARS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_PASSENGERCARS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_PERSONS_INIT"])
def STG_SLS_PERSONS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_PERSONS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_SHOPS_INIT"])
def STG_SLS_SHOPS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_SLS_SHOPS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_PURCHASEDBICYCLE_INIT"])
def STG_DL_SLS_PURCHASEDBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_DL_SLS_PURCHASEDBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_PURCHASEDEBICYCLE_INIT"])
def STG_DL_SLS_PURCHASEDEBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_DL_SLS_PURCHASEDEBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_PURCHASEDMCYCLE_INIT"])
def STG_DL_SLS_PURCHASEDMCYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_DL_SLS_PURCHASEDMCYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["EXT_SLS_PURCHASEDPASSCAR_INIT"])
def STG_DL_SLS_PURCHASEDPASSCAR_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.STG_DL_SLS_PURCHASEDPASSCAR_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_ELECTRICBICYCLES_INIT"])
def HUB_SLS_ELECTRIC_BICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_ELECTRIC_BICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_PASSENGERCARS_INIT"])
def HUB_SLS_PASSENGER_CARS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_PASSENGER_CARS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_SHOPS_INIT"])
def HUB_SLS_SHOPS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_SHOPS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_BICYCLES_INIT"])
def HUB_SLS_BICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_BICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_ADDRESSES_INIT"])
def HUB_SLS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_MOTORCYCLES_INIT"])
def HUB_SLS_MOTORCYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_MOTORCYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_INVOICES_INIT"])
def HUB_SLS_INVOICES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_INVOICES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_PERSONS_INIT"])
def HUB_SLS_PERSONS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.HUB_SLS_PERSONS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_INVOICES_INIT"])
def SAT_SLS_INVOICES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_INVOICES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_INVOICELINEITEMS_INIT"])
def SAT_SLS_INVOICELINEITEMS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_INVOICELINEITEMS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_PERSONS_INIT"])
def SAT_SLS_PERSONS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_PERSONS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_BICYCLES_INIT"])
def SAT_SLS_BICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_BICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_ADDRESSES_INIT"])
def SAT_SLS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_PASSENGERCARS_INIT"])
def SAT_SLS_PASSENGERCARS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_PASSENGERCARS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_MOTORCYCLES_INIT"])
def SAT_SLS_MOTORCYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_MOTORCYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_ELECTRICBICYCLES_INIT"])
def SAT_SLS_ELECTRICBICYCLES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_ELECTRICBICYCLES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_SHOPS_INIT"])
def SAT_SLS_SHOPS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.SAT_SLS_SHOPS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_PERSONS_INIT"])
def LNK_SLS_PERSONS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LNK_SLS_PERSONS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_SHOPS_INIT"])
def LNK_SLS_SHOPS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LNK_SLS_SHOPS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_INVOICES_INIT"])
def LNK_SLS_INVOICES_PERSONS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LNK_SLS_INVOICES_PERSONS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_PERSONS_INIT"])
def LKS_SLS_PERSONS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LKS_SLS_PERSONS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_INVOICES_INIT"])
def LKS_SLS_INVOICES_PERSONS_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LKS_SLS_INVOICES_PERSONS_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_SLS_SHOPS_INIT"])
def LKS_SLS_SHOPS_ADDRESSES_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LKS_SLS_SHOPS_ADDRESSES_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDBICYCLE_INIT"])
def LND_SLS_PURCHASED_BICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LND_SLS_PURCHASED_BICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDEBICYCLE_INIT"])
def LND_SLS_PURCHASED_EBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LND_SLS_PURCHASED_EBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDPASSCAR_INIT"])
def LND_SLS_PURCHASED_PASSCAR_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LND_SLS_PURCHASED_PASSCAR_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDMCYCLE_INIT"])
def LND_SLS_PURCHASED_MCYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LND_SLS_PURCHASED_MCYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDEBICYCLE_INIT"])
def LDS_SLS_PURCHASEDEBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LDS_SLS_PURCHASEDEBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDMCYCLE_INIT"])
def LDS_SLS_PURCHASEDMCYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LDS_SLS_PURCHASEDMCYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDPASSCAR_INIT"])
def LDS_SLS_PURCHASEDPASSCAR_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LDS_SLS_PURCHASEDPASSCAR_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")

@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["STG_DL_SLS_PURCHASEDBICYCLE_INIT"])
def LDS_SLS_PURCHASEDBICYCLE_INIT(context, snowflake: SnowflakeResource) -> None:
    query = f"""
        CALL SPEEDSHOP_PROC.LDS_SLS_PURCHASEDBICYCLE_INIT();
    """

     # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")



@asset(group_name="init_sales", compute_kind="Execute_Procedure", deps=["HUB_SLS_ADDRESSES_INIT", "HUB_SLS_BICYCLES_INIT", "HUB_SLS_ELECTRIC_BICYCLES_INIT", "HUB_SLS_INVOICES_INIT", "HUB_SLS_MOTORCYCLES_INIT", "HUB_SLS_PASSENGER_CARS_INIT", "HUB_SLS_PERSONS_INIT", "HUB_SLS_SHOPS_INIT", "LDS_SLS_PURCHASEDBICYCLE_INIT", "LDS_SLS_PURCHASEDEBICYCLE_INIT", "LDS_SLS_PURCHASEDMCYCLE_INIT", "LDS_SLS_PURCHASEDPASSCAR_INIT", "LKS_SLS_INVOICES_PERSONS_INIT", "LKS_SLS_PERSONS_ADDRESSES_INIT", "LKS_SLS_SHOPS_ADDRESSES_INIT", "LND_SLS_PURCHASED_BICYCLE_INIT", "LND_SLS_PURCHASED_EBICYCLE_INIT", "LND_SLS_PURCHASED_MCYCLE_INIT", "LND_SLS_PURCHASED_PASSCAR_INIT", "LNK_SLS_INVOICES_PERSONS_INIT", "LNK_SLS_PERSONS_ADDRESSES_INIT", "LNK_SLS_SHOPS_ADDRESSES_INIT", "SAT_SLS_ADDRESSES_INIT", "SAT_SLS_BICYCLES_INIT", "SAT_SLS_ELECTRICBICYCLES_INIT", "SAT_SLS_INVOICELINEITEMS_INIT", "SAT_SLS_INVOICES_INIT", "SAT_SLS_MOTORCYCLES_INIT", "SAT_SLS_PASSENGERCARS_INIT", "SAT_SLS_PERSONS_INIT", "SAT_SLS_SHOPS_INIT"])
def FMC_UPD_RUN_STATUS_FL_SLS(context, snowflake: SnowflakeResource) -> None:
    # Procedure arguments
    load_cycle_id = '100'
    success_flag = '1'
    
    # Construct the query with string formatting
    query = f"CALL SPEEDSHOP_PROC.SET_FMC_MTD_FL_INIT_SLS('{str(load_cycle_id)}', '{str(success_flag)}');"

    # Log the query for debugging purposes
    context.log.info(f"Executing query: {query}")

    # Execute the query using the Snowflake connection
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

    context.log.info("Stored procedure executed successfully.")



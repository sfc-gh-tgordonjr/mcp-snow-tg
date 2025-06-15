from typing import Dict, List, Optional, Union
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from mcp_server_snowflake.utils import get_snowflake_connection

class DMLManager:
    """A class to manage DML operations in Snowflake."""
    
    def __init__(
        self,
        account_identifier: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection: Optional[snowflake.connector.SnowflakeConnection] = None,
        **kwargs
    ):
        """Initialize the DML manager.
        
        Args:
            account_identifier: Snowflake account identifier
            username: Snowflake username
            password: Snowflake password or PAT
            connection: Optional existing Snowflake connection
            **kwargs: Additional connection parameters
        """
        self.connection = connection or get_snowflake_connection(
            account_identifier=account_identifier,
            username=username,
            password=password,
            **kwargs
        )
        
    def execute_dml(self, dml: str) -> Dict[str, Union[bool, str, List[str], int]]:
        """Execute a DML statement and return the result.
        
        Args:
            dml: The DML statement to execute
            
        Returns:
            Dict containing:
                - success: Boolean indicating if the operation was successful
                - message: Status message
                - results: List of results if any were returned
                - rows_affected: Number of rows affected by the operation
        """
        try:
            cursor: SnowflakeCursor = self.connection.cursor()
            cursor.execute(dml)
            results = cursor.fetchall()
            rows_affected = cursor.rowcount
            
            return {
                "success": True,
                "message": "DML operation executed successfully",
                "results": [str(row) for row in results] if results else [],
                "rows_affected": rows_affected
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error executing DML: {str(e)}",
                "results": [],
                "rows_affected": 0
            }
            
    def select_data(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Union[bool, str, List[str], int]]:
        """Select data from a table.
        
        Args:
            table_name: Fully qualified table name (database.schema.table)
            columns: Optional list of columns to select
            where_clause: Optional WHERE clause
            order_by: Optional list of columns to order by
            limit: Optional number of rows to return
            offset: Optional number of rows to skip
            
        Returns:
            Dict containing operation status
        """
        # Parse table name to ensure schema is properly formatted
        parts = table_name.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            schema_name = f"{database}.{schema}"
            table_name = f"{schema_name}.{table}"
        elif len(parts) == 2:
            raise ValueError("Table name must be fully qualified as 'database.schema.table'")
        else:
            raise ValueError("Invalid table name format. Must be 'database.schema.table'")
            
        cols = "*" if not columns else ", ".join(columns)
        dml = f"SELECT {cols} FROM {table_name}"
        
        if where_clause:
            dml += f" WHERE {where_clause}"
            
        if order_by:
            dml += f" ORDER BY {', '.join(order_by)}"
            
        if limit is not None:
            dml += f" LIMIT {limit}"
            
        if offset is not None:
            dml += f" OFFSET {offset}"
            
        return self.execute_dml(dml)
        
    def insert_data(
        self,
        table_name: str,
        columns: List[str],
        values: List[Union[str, int, float, bool, None]]
    ) -> Dict[str, Union[bool, str, List[str], int]]:
        """Insert data into a table.
        
        Args:
            table_name: Fully qualified table name (database.schema.table)
            columns: List of column names
            values: List of values to insert
            
        Returns:
            Dict containing operation status
        """
        # Parse table name to ensure schema is properly formatted
        parts = table_name.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            schema_name = f"{database}.{schema}"
            table_name = f"{schema_name}.{table}"
        elif len(parts) == 2:
            raise ValueError("Table name must be fully qualified as 'database.schema.table'")
        else:
            raise ValueError("Invalid table name format. Must be 'database.schema.table'")
            
        # Format values properly based on type
        formatted_values = []
        for val in values:
            if val is None:
                formatted_values.append("NULL")
            elif isinstance(val, (int, float)):
                formatted_values.append(str(val))
            elif isinstance(val, bool):
                formatted_values.append("TRUE" if val else "FALSE")
            else:
                formatted_values.append(f"'{str(val)}'")
                
        dml = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(formatted_values)})
        """
        return self.execute_dml(dml)
        
    def update_data(
        self,
        table_name: str,
        set_columns: List[str],
        set_values: List[Union[str, int, float, bool, None]],
        where_clause: str
    ) -> Dict[str, Union[bool, str, List[str], int]]:
        """Update data in a table.
        
        Args:
            table_name: Fully qualified table name (database.schema.table)
            set_columns: List of column names to update
            set_values: List of new values
            where_clause: WHERE clause to identify rows to update
            
        Returns:
            Dict containing operation status
        """
        # Parse table name to ensure schema is properly formatted
        parts = table_name.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            schema_name = f"{database}.{schema}"
            table_name = f"{schema_name}.{table}"
        elif len(parts) == 2:
            raise ValueError("Table name must be fully qualified as 'database.schema.table'")
        else:
            raise ValueError("Invalid table name format. Must be 'database.schema.table'")
            
        if len(set_columns) != len(set_values):
            return {
                "success": False,
                "message": "Number of columns does not match number of values",
                "results": [],
                "rows_affected": 0
            }
            
        # Format values properly based on type
        set_clauses = []
        for col, val in zip(set_columns, set_values):
            if val is None:
                set_clauses.append(f"{col} = NULL")
            elif isinstance(val, (int, float)):
                set_clauses.append(f"{col} = {str(val)}")
            elif isinstance(val, bool):
                set_clauses.append(f"{col} = {'TRUE' if val else 'FALSE'}")
            else:
                set_clauses.append(f"{col} = '{str(val)}'")
                
        dml = f"""
        UPDATE {table_name}
        SET {', '.join(set_clauses)}
        WHERE {where_clause}
        """
        return self.execute_dml(dml)
        
    def delete_data(
        self,
        table_name: str,
        where_clause: str
    ) -> Dict[str, Union[bool, str, List[str], int]]:
        """Delete data from a table.
        
        Args:
            table_name: Fully qualified table name (database.schema.table)
            where_clause: WHERE clause to identify rows to delete
            
        Returns:
            Dict containing operation status
        """
        # Parse table name to ensure schema is properly formatted
        parts = table_name.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            schema_name = f"{database}.{schema}"
            table_name = f"{schema_name}.{table}"
        elif len(parts) == 2:
            raise ValueError("Table name must be fully qualified as 'database.schema.table'")
        else:
            raise ValueError("Invalid table name format. Must be 'database.schema.table'")
            
        dml = f"""
        DELETE FROM {table_name}
        WHERE {where_clause}
        """
        return self.execute_dml(dml)
        
    def merge_data(
        self,
        target_table: str,
        source_table: str,
        merge_condition: str,
        match_actions: List[Dict[str, Union[str, List[str], List[Union[str, int, float, bool, None]]]]],
        not_match_actions: Optional[List[Dict[str, Union[str, List[str], List[Union[str, int, float, bool, None]]]]]] = None
    ) -> Dict[str, Union[bool, str, List[str], int]]:
        """Perform a MERGE operation.
        
        Args:
            target_table: Fully qualified target table name
            source_table: Source table or subquery
            merge_condition: ON clause condition for matching records
            match_actions: List of actions when records match (UPDATE/DELETE)
            not_match_actions: Optional list of actions when records don't match (INSERT)
            
        Returns:
            Dict containing operation status
        """
        # Parse target table name to ensure schema is properly formatted
        parts = target_table.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            schema_name = f"{database}.{schema}"
            target_table = f"{schema_name}.{table}"
        elif len(parts) == 2:
            raise ValueError("Target table name must be fully qualified as 'database.schema.table'")
        else:
            raise ValueError("Invalid target table name format. Must be 'database.schema.table'")
            
        dml = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON {merge_condition}
        """
        
        # Add WHEN MATCHED clauses
        for action in match_actions:
            action_type = action["action"]
            if action_type == "UPDATE":
                columns = action["columns"]
                values = action["values"]
                if len(columns) != len(values):
                    return {
                        "success": False,
                        "message": "Number of columns does not match number of values in WHEN MATCHED UPDATE action",
                        "results": [],
                        "rows_affected": 0
                    }
                    
                # Format values properly based on type
                set_clauses = []
                for col, val in zip(columns, values):
                    if val is None:
                        set_clauses.append(f"{col} = NULL")
                    elif isinstance(val, (int, float)):
                        set_clauses.append(f"{col} = {str(val)}")
                    elif isinstance(val, bool):
                        set_clauses.append(f"{col} = {'TRUE' if val else 'FALSE'}")
                    else:
                        set_clauses.append(f"{col} = '{str(val)}'")
                        
                dml += f"\nWHEN MATCHED THEN UPDATE SET {', '.join(set_clauses)}"
                
            elif action_type == "DELETE":
                dml += "\nWHEN MATCHED THEN DELETE"
                
        # Add WHEN NOT MATCHED clauses
        if not_match_actions:
            for action in not_match_actions:
                action_type = action["action"]
                if action_type == "INSERT":
                    columns = action["columns"]
                    values = action["values"]
                    if len(columns) != len(values):
                        return {
                            "success": False,
                            "message": "Number of columns does not match number of values in WHEN NOT MATCHED INSERT action",
                            "results": [],
                            "rows_affected": 0
                        }
                        
                    # Format values properly based on type
                    formatted_values = []
                    for val in values:
                        if val is None:
                            formatted_values.append("NULL")
                        elif isinstance(val, (int, float)):
                            formatted_values.append(str(val))
                        elif isinstance(val, bool):
                            formatted_values.append("TRUE" if val else "FALSE")
                        else:
                            formatted_values.append(f"'{str(val)}'")
                            
                    dml += f"\nWHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join(formatted_values)})"
                    
        return self.execute_dml(dml) 
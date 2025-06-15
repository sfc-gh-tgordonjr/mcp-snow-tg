from typing import Dict, List, Optional, Union
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from mcp_server_snowflake.utils import format_schema_name, get_snowflake_connection

class DDLManager:
    """A class to manage DDL operations in Snowflake."""
    
    def __init__(
        self,
        account_identifier: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection: Optional[snowflake.connector.SnowflakeConnection] = None,
        **kwargs
    ):
        """Initialize the DDL manager.
        
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
        
    def execute_ddl(self, ddl_statement: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Execute a DDL statement and return the result.
        
        Args:
            ddl_statement: The DDL statement to execute
            
        Returns:
            Dict containing:
                - success: Boolean indicating if the operation was successful
                - message: Status message
                - results: List of results if any were returned
        """
        try:
            cursor: SnowflakeCursor = self.connection.cursor()
            results = cursor.execute(ddl_statement).fetchall()
            
            return {
                "success": True,
                "message": "DDL operation executed successfully",
                "results": [str(row) for row in results] if results else []
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error executing DDL: {str(e)}",
                "results": []
            }
            
    def create_database(self, database_name: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Create a new database.
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            Dict containing operation status
        """
        ddl = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        return self.execute_ddl(ddl)
        
    def create_schema(self, database_name: str, schema_name: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Create a new schema in the specified database.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema to create
            
        Returns:
            Dict containing operation status
        """
        ddl = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
        return self.execute_ddl(ddl)
        
    def create_table(self, database_name: str, schema_name: str, table_name: str, 
                    columns: List[Dict[str, str]]) -> Dict[str, Union[bool, str, List[str]]]:
        """Create a new table with specified columns.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table to create
            columns: List of column definitions, each containing 'name' and 'type'
            
        Returns:
            Dict containing operation status
        """
        column_defs = [f"{col['name']} {col['type']}" for col in columns]
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            {', '.join(column_defs)}
        )
        """
        return self.execute_ddl(ddl)
        
    def drop_object(self, object_type: str, object_name: str, 
                   cascade: bool = False) -> Dict[str, Union[bool, str, List[str]]]:
        """Drop a database object (database, schema, table, etc.).
        
        Args:
            object_type: Type of object to drop (DATABASE, SCHEMA, TABLE, etc.)
            object_name: Fully qualified name of the object
            cascade: Whether to cascade the drop operation
            
        Returns:
            Dict containing operation status
        """
        cascade_str = "CASCADE" if cascade else ""
        ddl = f"DROP {object_type} IF EXISTS {object_name} {cascade_str}"
        return self.execute_ddl(ddl)

    def alter_table(
        self,
        table_name: str,
        alter_type: str,
        column_name: Optional[str] = None,
        new_name: Optional[str] = None,
        data_type: Optional[str] = None,
        default_value: Optional[str] = None,
        not_null: Optional[bool] = None
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Alter a table's structure.
        
        Args:
            table_name: Fully qualified table name (database.schema.table)
            alter_type: Type of alteration (ADD, DROP, RENAME, ALTER)
            column_name: Name of the column to alter
            new_name: New name for RENAME operations
            data_type: Data type for ADD or ALTER operations
            default_value: Default value for the column
            not_null: Whether the column should be NOT NULL
            
        Returns:
            Dict containing operation status
        """
        if alter_type == "ADD":
            if not (column_name and data_type):
                raise ValueError("Column name and data type required for ADD operation")
            ddl = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}"
            if default_value is not None:
                ddl += f" DEFAULT {default_value}"
            if not_null:
                ddl += " NOT NULL"
                
        elif alter_type == "DROP":
            if not column_name:
                raise ValueError("Column name required for DROP operation")
            ddl = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
            
        elif alter_type == "RENAME":
            if not (column_name and new_name):
                raise ValueError("Column name and new name required for RENAME operation")
            ddl = f"ALTER TABLE {table_name} RENAME COLUMN {column_name} TO {new_name}"
            
        elif alter_type == "ALTER":
            if not (column_name and data_type):
                raise ValueError("Column name and data type required for ALTER operation")
            ddl = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DATA TYPE {data_type}"
            
        else:
            raise ValueError(f"Unsupported alter type: {alter_type}")
            
        return self.execute_ddl(ddl)
        
    def alter_schema(
        self,
        schema_name: str,
        new_name: Optional[str] = None,
        new_database: Optional[str] = None
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Alter a schema.
        
        Args:
            schema_name: Fully qualified schema name (database.schema)
            new_name: New name for the schema
            new_database: New database for the schema
            
        Returns:
            Dict containing operation status
        """
        # Ensure schema_name is properly formatted
        schema_name = format_schema_name(schema_name)
        
        if new_name:
            ddl = f"ALTER SCHEMA {schema_name} RENAME TO {new_name}"
        elif new_database:
            ddl = f"ALTER SCHEMA {schema_name} SET DATA TYPE {new_database}"
        else:
            raise ValueError("Either new_name or new_database must be provided")
            
        return self.execute_ddl(ddl)
        
    def alter_database(
        self,
        database_name: str,
        new_name: str
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Alter a database.
        
        Args:
            database_name: Name of the database to alter
            new_name: New name for the database
            
        Returns:
            Dict containing operation status
        """
        ddl = f"ALTER DATABASE {database_name} RENAME TO {new_name}"
        return self.execute_ddl(ddl) 
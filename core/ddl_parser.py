"""
DDL Parser using regex for reliable SQL parsing.

This module provides a clean, reliable approach to parsing DDL files
using regex patterns that work without external dependencies.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class DataType(Enum):
    """Supported SQL data types"""
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE_PRECISION"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    UUID = "UUID"
    JSON = "JSON"
    JSONB = "JSONB"
    INET = "INET"
    CIDR = "CIDR"
    MACADDR = "MACADDR"
    BYTEA = "BYTEA"
    INTERVAL = "INTERVAL"

@dataclass
class Column:
    name: str
    data_type: DataType
    nullable: bool = True
    default_value: Optional[str] = None
    constraints: List[str] = None
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: Optional[str] = None
    foreign_column: Optional[str] = None
    
    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []

@dataclass
class Table:
    name: str
    columns: List[Column]
    primary_keys: List[str]
    foreign_keys: List[Tuple[str, str, str]]  # (column, foreign_table, foreign_column)

class DDLParser:
    """DDL parser using regex for reliable SQL parsing"""
    
    def __init__(self):
        # Map SQL data types to our enum
        self.type_mapping = {
            'INT': DataType.INTEGER,
            'INTEGER': DataType.INTEGER,
            'BIGINT': DataType.BIGINT,
            'SMALLINT': DataType.SMALLINT,
            'SERIAL': DataType.SERIAL,
            'BIGSERIAL': DataType.BIGSERIAL,
            'DECIMAL': DataType.DECIMAL,
            'NUMERIC': DataType.NUMERIC,
            'REAL': DataType.REAL,
            'DOUBLE': DataType.DOUBLE_PRECISION,
            'DOUBLE_PRECISION': DataType.DOUBLE_PRECISION,
            'VARCHAR': DataType.VARCHAR,
            'CHAR': DataType.CHAR,
            'TEXT': DataType.TEXT,
            'BOOLEAN': DataType.BOOLEAN,
            'BOOL': DataType.BOOLEAN,
            'DATE': DataType.DATE,
            'TIME': DataType.TIME,
            'TIMESTAMP': DataType.TIMESTAMP,
            'TIMESTAMPTZ': DataType.TIMESTAMPTZ,
            'UUID': DataType.UUID,
            'JSON': DataType.JSON,
            'JSONB': DataType.JSONB,
            'INET': DataType.INET,
            'CIDR': DataType.CIDR,
            'MACADDR': DataType.MACADDR,
            'BYTEA': DataType.BYTEA,
            'INTERVAL': DataType.INTERVAL,
        }
    
    def parse_ddl(self, ddl_content: str) -> List[Table]:
        """Parse DDL content and return list of Table objects"""
        try:
            # Clean the DDL content
            ddl_content = self._clean_ddl(ddl_content)
            
            # Parse using regex with proper parenthesis handling
            tables = []
            create_table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\('
            
            for match in re.finditer(create_table_pattern, ddl_content, re.IGNORECASE):
                table_name = match.group(1)
                start_pos = match.end()
                
                # Find the matching closing parenthesis
                paren_count = 1
                pos = start_pos
                while pos < len(ddl_content) and paren_count > 0:
                    if ddl_content[pos] == '(':
                        paren_count += 1
                    elif ddl_content[pos] == ')':
                        paren_count -= 1
                    pos += 1
                
                if paren_count == 0:
                    columns_text = ddl_content[start_pos:pos-1]
                    table = self._parse_table_columns(table_name, columns_text)
                    if table:
                        tables.append(table)
            
            logger.info(f"Successfully parsed {len(tables)} tables from DDL")
            return tables
            
        except Exception as e:
            logger.error(f"Error parsing DDL: {str(e)}")
            raise ValueError(f"Failed to parse DDL: {str(e)}")
    
    def _clean_ddl(self, ddl_content: str) -> str:
        """Clean and normalize DDL content"""
        # Remove comments
        ddl_content = re.sub(r'--.*$', '', ddl_content, flags=re.MULTILINE)
        ddl_content = re.sub(r'/\*.*?\*/', '', ddl_content, flags=re.DOTALL)
        
        # Normalize whitespace but preserve structure
        ddl_content = re.sub(r'\n\s*\n', '\n', ddl_content)  # Remove empty lines
        ddl_content = re.sub(r'[ \t]+', ' ', ddl_content)  # Normalize spaces and tabs
        ddl_content = ddl_content.strip()
        
        return ddl_content
    
    def _parse_table_columns(self, table_name: str, columns_text: str) -> Optional[Table]:
        """Parse table columns from column definition text"""
        try:
            columns = []
            primary_keys = []
            foreign_keys = []
            
            # Split by comma, but be careful with nested parentheses
            column_definitions = self._split_column_definitions(columns_text)
            
            for col_def in column_definitions:
                col_def = col_def.strip()
                if not col_def:
                    continue
                
                # Parse column definition
                column = self._parse_column_definition(col_def)
                if column:
                    columns.append(column)
                    
                    # Check for primary key
                    if 'PRIMARY KEY' in col_def.upper():
                        primary_keys.append(column.name)
                    
                    # Check for foreign key
                    fk_match = re.search(r'FOREIGN\s+KEY\s*\((\w+)\)\s*REFERENCES\s+(\w+)\s*\((\w+)\)', 
                                       col_def, re.IGNORECASE)
                    if fk_match:
                        foreign_keys.append((fk_match.group(1), fk_match.group(2), fk_match.group(3)))
            
            # Handle separate PRIMARY KEY constraint
            pk_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', columns_text, re.IGNORECASE)
            if pk_match:
                pk_columns = [col.strip() for col in pk_match.group(1).split(',')]
                primary_keys.extend(pk_columns)
                # Mark columns as primary keys
                for col in columns:
                    if col.name in pk_columns:
                        col.is_primary_key = True
            
            # Handle separate FOREIGN KEY constraints (avoid duplicates)
            fk_matches = re.finditer(r'FOREIGN\s+KEY\s*\((\w+)\)\s*REFERENCES\s+(\w+)\s*\((\w+)\)', 
                                   columns_text, re.IGNORECASE)
            for fk_match in fk_matches:
                fk_column = fk_match.group(1)
                fk_table = fk_match.group(2)
                fk_column_ref = fk_match.group(3)
                
                # Check if this foreign key is already added (avoid duplicates)
                fk_tuple = (fk_column, fk_table, fk_column_ref)
                if fk_tuple not in foreign_keys:
                    foreign_keys.append(fk_tuple)
                    # Mark column as foreign key
                    for col in columns:
                        if col.name == fk_column:
                            col.is_foreign_key = True
                            col.foreign_table = fk_table
                            col.foreign_column = fk_column_ref
            
            return Table(
                name=table_name,
                columns=columns,
                primary_keys=list(set(primary_keys)),
                foreign_keys=foreign_keys
            )
            
        except Exception as e:
            logger.error(f"Error parsing table {table_name}: {str(e)}")
            return None
    
    def _split_column_definitions(self, columns_text: str) -> List[str]:
        """Split column definitions by comma, respecting nested parentheses"""
        definitions = []
        current_def = ""
        paren_count = 0
        
        for char in columns_text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                definitions.append(current_def.strip())
                current_def = ""
                continue
            
            current_def += char
        
        if current_def.strip():
            definitions.append(current_def.strip())
        
        return definitions
    
    def _parse_column_definition(self, col_def: str) -> Optional[Column]:
        """Parse a single column definition"""
        try:
            # Extract column name and type with parameters
            match = re.match(r'(\w+)\s+(\w+)(?:\(([^)]*)\))?', col_def)
            if not match:
                return None
            
            column_name = match.group(1)
            data_type_str = match.group(2).upper()
            type_params = match.group(3) if match.group(3) else None
            
            
            # Map to our enum
            data_type = self.type_mapping.get(data_type_str, DataType.TEXT)
            
            # Extract length, precision, and scale from type parameters
            length = None
            precision = None
            scale = None
            
            if type_params:
                if ',' in type_params:
                    # DECIMAL(10,2) or NUMERIC(10,2)
                    parts = type_params.split(',')
                    if len(parts) == 2:
                        precision = int(parts[0].strip())
                        scale = int(parts[1].strip())
                else:
                    # VARCHAR(50) or CHAR(10)
                    try:
                        length = int(type_params.strip())
                    except ValueError:
                        pass
            
            # Check for NOT NULL
            nullable = 'NOT NULL' not in col_def.upper()
            
            # Extract default value
            default_value = None
            default_match = re.search(r'DEFAULT\s+([^,\s]+)', col_def, re.IGNORECASE)
            if default_match:
                default_value = default_match.group(1)
            
            # Check for PRIMARY KEY
            is_primary_key = 'PRIMARY KEY' in col_def.upper()
            
            # Check for FOREIGN KEY
            is_foreign_key = False
            foreign_table = None
            foreign_column = None
            fk_match = re.search(r'REFERENCES\s+(\w+)\s*\((\w+)\)', col_def, re.IGNORECASE)
            if fk_match:
                is_foreign_key = True
                foreign_table = fk_match.group(1)
                foreign_column = fk_match.group(2)
            
            # Extract constraints
            constraints = []
            if 'UNIQUE' in col_def.upper():
                constraints.append('UNIQUE')
            if 'CHECK' in col_def.upper():
                check_match = re.search(r'CHECK\s*\(([^)]+)\)', col_def, re.IGNORECASE)
                if check_match:
                    constraints.append(f"CHECK({check_match.group(1)})")
            
            return Column(
                name=column_name,
                data_type=data_type,
                nullable=nullable,
                default_value=default_value,
                constraints=constraints,
                length=length,
                precision=precision,
                scale=scale,
                is_primary_key=is_primary_key,
                is_foreign_key=is_foreign_key,
                foreign_table=foreign_table,
                foreign_column=foreign_column
            )
            
        except Exception as e:
            logger.error(f"Error parsing column definition '{col_def}': {str(e)}")
            return None
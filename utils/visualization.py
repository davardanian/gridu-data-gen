# utils/visualization.py
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from typing import Optional, Dict, Any, List
import streamlit as st
from core.observability import observability

class VisualizationManager:
    """Manages data visualization using Seaborn and Matplotlib"""
    
    def __init__(self):
        self.observability = observability
        self._setup_plotting_style()
    
    def _setup_plotting_style(self):
        """Setup plotting style for consistent visualizations"""
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (10, 6)
        plt.rcParams['font.size'] = 10
    
    def create_chart(self, data: pd.DataFrame, chart_type: str, 
                    x_column: Optional[str] = None, y_column: Optional[str] = None,
                    **kwargs) -> Optional[plt.Figure]:
        """Create various types of charts"""
        try:
            self.observability.log_info(f"Creating {chart_type} chart")
            
            if chart_type == "bar":
                return self._create_bar_chart(data, x_column, y_column, **kwargs)
            elif chart_type == "line":
                return self._create_line_chart(data, x_column, y_column, **kwargs)
            elif chart_type == "scatter":
                return self._create_scatter_chart(data, x_column, y_column, **kwargs)
            elif chart_type == "histogram":
                return self._create_histogram(data, x_column, **kwargs)
            elif chart_type == "heatmap":
                return self._create_heatmap(data, **kwargs)
            elif chart_type == "box":
                return self._create_box_plot(data, x_column, y_column, **kwargs)
            else:
                self.observability.log_error(f"Unknown chart type: {chart_type}")
                return None
                
        except Exception as e:
            self.observability.log_error(f"Chart creation failed: {str(e)}")
            return None
    
    def _create_bar_chart(self, data: pd.DataFrame, x_column: str, y_column: str, **kwargs) -> plt.Figure:
        """Create bar chart using seaborn"""
        fig, ax = plt.subplots(figsize=(12, 8))
        
        try:
            # Handle different data scenarios
            if data.empty:
                ax.text(0.5, 0.5, 'No data available for visualization', 
                       ha='center', va='center', transform=ax.transAxes, fontsize=14)
                ax.set_title('Bar Chart - No Data')
                return fig
            
            # If we have both x and y columns, create a proper bar chart
            if y_column and y_column in data.columns and x_column in data.columns:
                # Ensure data is properly formatted
                if data[x_column].dtype == 'object' or data[x_column].dtype.name == 'category':
                    # Categorical x-axis with numerical y-axis
                    sns.barplot(data=data, x=x_column, y=y_column, ax=ax)
                    ax.set_title(f'Bar Chart: {y_column} by {x_column}')
                else:
                    # Both numerical - create a simple bar chart
                    sns.barplot(data=data, x=x_column, y=y_column, ax=ax)
                    ax.set_title(f'Bar Chart: {y_column} vs {x_column}')
            
            # If we only have x_column (categorical data)
            elif x_column in data.columns and data[x_column].dtype == 'object':
                # Count occurrences of each category
                value_counts = data[x_column].value_counts().head(15)  # Top 15 categories
                if len(value_counts) > 0:
                    sns.barplot(x=value_counts.values, y=value_counts.index, ax=ax)
                    ax.set_xlabel('Count')
                    ax.set_ylabel(x_column)
                    ax.set_title(f'Bar Chart: Count of {x_column}')
                else:
                    ax.text(0.5, 0.5, f'No data in column: {x_column}', 
                           ha='center', va='center', transform=ax.transAxes)
                    ax.set_title(f'Bar Chart: {x_column}')
            
            # If we only have numerical data
            elif x_column in data.columns and data[x_column].dtype in ['int64', 'float64']:
                # Create a histogram-like bar chart for numerical data
                bins = min(20, len(data[x_column].unique()))
                if bins > 1:
                    data[x_column].hist(bins=bins, ax=ax, alpha=0.7, edgecolor='black')
                    ax.set_xlabel(x_column)
                    ax.set_ylabel('Frequency')
                    ax.set_title(f'Distribution of {x_column}')
                else:
                    # Single value - show as a single bar
                    value = data[x_column].iloc[0]
                    ax.bar([0], [1], label=f'{x_column}: {value}')
                    ax.set_xlabel('')
                    ax.set_ylabel('Count')
                    ax.set_title(f'Single Value: {x_column} = {value}')
                    ax.set_xticks([])
            
            else:
                # Fallback - try to create a simple bar chart with available data
                if len(data.columns) >= 2:
                    col1, col2 = data.columns[0], data.columns[1]
                    sns.barplot(data=data, x=col1, y=col2, ax=ax)
                    ax.set_title(f'Bar Chart: {col2} by {col1}')
                else:
                    ax.text(0.5, 0.5, 'Insufficient data for bar chart', 
                           ha='center', va='center', transform=ax.transAxes)
                    ax.set_title('Bar Chart - Insufficient Data')
            
            # Improve appearance
            ax.grid(True, alpha=0.3)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
        except Exception as e:
            # Error handling - show error message in chart
            ax.text(0.5, 0.5, f'Error creating bar chart: {str(e)}', 
                   ha='center', va='center', transform=ax.transAxes, fontsize=12)
            ax.set_title('Bar Chart - Error')
            self.observability.log_error(f"Bar chart creation failed: {str(e)}")
        
        return fig
    
    def _create_line_chart(self, data: pd.DataFrame, x_column: str, y_column: str, **kwargs) -> plt.Figure:
        """Create line chart"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if y_column and y_column in data.columns:
            # Plot line chart
            if data[x_column].dtype in ['datetime64[ns]', 'object']:
                # Try to convert to datetime if possible
                try:
                    data[x_column] = pd.to_datetime(data[x_column])
                except:
                    pass
            
            sns.lineplot(data=data, x=x_column, y=y_column, ax=ax)
            ax.set_title(f'Line Chart: {y_column} vs {x_column}')
        else:
            # Create time series plot
            if data[x_column].dtype in ['datetime64[ns]']:
                data.set_index(x_column).plot(ax=ax)
                ax.set_title(f'Time Series: {x_column}')
            else:
                # Line plot
                ax.plot(data[x_column])
                ax.set_xlabel(x_column)
                ax.set_ylabel('Value')
                ax.set_title(f'Line Chart: {x_column}')
        
        plt.tight_layout()
        return fig
    
    def _create_scatter_chart(self, data: pd.DataFrame, x_column: str, y_column: str, **kwargs) -> plt.Figure:
        """Create scatter plot"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if y_column and y_column in data.columns:
            sns.scatterplot(data=data, x=x_column, y=y_column, ax=ax)
            ax.set_title(f'Scatter Plot: {y_column} vs {x_column}')
        else:
            # Create scatter plot with index
            ax.scatter(range(len(data)), data[x_column])
            ax.set_xlabel('Index')
            ax.set_ylabel(x_column)
            ax.set_title(f'Scatter Plot: {x_column}')
        
        plt.tight_layout()
        return fig
    
    def _create_histogram(self, data: pd.DataFrame, column: str, **kwargs) -> plt.Figure:
        """Create histogram"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Handle different data types
        if data[column].dtype in ['int64', 'float64']:
            # Numerical data
            sns.histplot(data=data, x=column, ax=ax, bins=20)
        else:
            # Categorical data - create bar chart instead
            value_counts = data[column].value_counts().head(15)
            sns.barplot(x=value_counts.values, y=value_counts.index, ax=ax)
            ax.set_xlabel('Count')
        
        ax.set_title(f'Distribution: {column}')
        plt.tight_layout()
        return fig
    
    def _create_heatmap(self, data: pd.DataFrame, **kwargs) -> plt.Figure:
        """Create correlation heatmap"""
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Select only numerical columns
        numerical_data = data.select_dtypes(include=['int64', 'float64'])
        
        if len(numerical_data.columns) < 2:
            # Not enough numerical columns for heatmap
            ax.text(0.5, 0.5, 'Not enough numerical columns for heatmap', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title('Correlation Heatmap')
            return fig
        
        # Calculate correlation matrix
        correlation_matrix = numerical_data.corr()
        
        # Create heatmap
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, ax=ax)
        ax.set_title('Correlation Heatmap')
        
        plt.tight_layout()
        return fig
    
    def _create_box_plot(self, data: pd.DataFrame, x_column: str, y_column: str, **kwargs) -> plt.Figure:
        """Create box plot"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if y_column and y_column in data.columns:
            # Box plot with categorical x-axis
            sns.boxplot(data=data, x=x_column, y=y_column, ax=ax)
            ax.set_title(f'Box Plot: {y_column} by {x_column}')
        else:
            # Single box plot
            sns.boxplot(data=data, y=x_column, ax=ax)
            ax.set_title(f'Box Plot: {x_column}')
        
        plt.tight_layout()
        return fig
    
    def determine_chart_type(self, query: str, data: pd.DataFrame) -> str:
        """Determine appropriate chart type based on query and data"""
        query_lower = query.lower()
        
        # Check for specific chart type requests
        if any(word in query_lower for word in ['bar', 'bar chart', 'bars']):
            return "bar"
        elif any(word in query_lower for word in ['line', 'line chart', 'trend', 'time series']):
            return "line"
        elif any(word in query_lower for word in ['scatter', 'scatter plot', 'correlation']):
            return "scatter"
        elif any(word in query_lower for word in ['histogram', 'distribution', 'frequency']):
            return "histogram"
        elif any(word in query_lower for word in ['heatmap', 'correlation matrix']):
            return "heatmap"
        elif any(word in query_lower for word in ['box', 'box plot', 'quartile']):
            return "box"
        
        # Special handling for grouped/categorical data
        if any(word in query_lower for word in ['based on', 'group by', 'distribution', 'demographics']):
            # Check if data has categorical data
            categorical_columns = data.select_dtypes(include=['object', 'category']).columns
            if len(categorical_columns) > 0:
                return "bar"  # Bar chart is best for categorical groups
            else:
                return "histogram"  # Histogram for continuous data
        
        # Auto-determine based on data characteristics
        numerical_columns = data.select_dtypes(include=['int64', 'float64']).columns
        categorical_columns = data.select_dtypes(include=['object', 'category']).columns
        
        if len(categorical_columns) > 0 and len(numerical_columns) > 0:
            # Mixed data - bar chart is usually best
            return "bar"
        elif len(numerical_columns) >= 2:
            # Multiple numerical columns - suggest scatter or heatmap
            if len(data) > 100:
                return "scatter"
            else:
                return "heatmap"
        elif len(numerical_columns) == 1:
            # Single numerical column - suggest histogram
            return "histogram"
        else:
            # Categorical data - suggest bar chart
            return "bar"
    
    def get_data_summary(self, data: pd.DataFrame) -> str:
        """Generate statistical summary of data"""
        try:
            summary_parts = []
            
            # Basic info
            summary_parts.append(f"Dataset Overview:")
            summary_parts.append(f"- Rows: {len(data)}")
            summary_parts.append(f"- Columns: {len(data.columns)}")
            summary_parts.append(f"- Memory Usage: {data.memory_usage(deep=True).sum() / 1024:.1f} KB")
            
            # Column information
            summary_parts.append(f"\nColumn Information:")
            for col in data.columns:
                dtype = str(data[col].dtype)
                null_count = data[col].isnull().sum()
                summary_parts.append(f"- {col}: {dtype} ({null_count} nulls)")
            
            # Numerical columns summary
            numerical_cols = data.select_dtypes(include=['int64', 'float64']).columns
            if len(numerical_cols) > 0:
                summary_parts.append(f"\nNumerical Columns Summary:")
                for col in numerical_cols:
                    stats = data[col].describe()
                    summary_parts.append(f"- {col}: mean={stats['mean']:.2f}, std={stats['std']:.2f}, min={stats['min']:.2f}, max={stats['max']:.2f}")
            
            # Categorical columns summary
            categorical_cols = data.select_dtypes(include=['object', 'category']).columns
            if len(categorical_cols) > 0:
                summary_parts.append(f"\nCategorical Columns Summary:")
                for col in categorical_cols:
                    unique_count = data[col].nunique()
                    top_value = data[col].value_counts().index[0] if len(data[col].value_counts()) > 0 else "N/A"
                    summary_parts.append(f"- {col}: {unique_count} unique values, most common: {top_value}")
            
            return "\n".join(summary_parts)
            
        except Exception as e:
            self.observability.log_error(f"Data summary generation failed: {str(e)}")
            return f"Error generating summary: {str(e)}"
    
    def suggest_visualizations(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Suggest appropriate visualizations for the data"""
        suggestions = []
        
        try:
            numerical_cols = data.select_dtypes(include=['int64', 'float64']).columns
            categorical_cols = data.select_dtypes(include=['object', 'category']).columns
            
            # Histogram for numerical columns
            for col in numerical_cols:
                suggestions.append({
                    "type": "histogram",
                    "title": f"Distribution of {col}",
                    "description": f"Shows the distribution of values in {col}",
                    "column": col
                })
            
            # Bar chart for categorical columns
            for col in categorical_cols:
                suggestions.append({
                    "type": "bar",
                    "title": f"Count of {col}",
                    "description": f"Shows the count of each category in {col}",
                    "column": col
                })
            
            # Scatter plot for numerical pairs
            if len(numerical_cols) >= 2:
                suggestions.append({
                    "type": "scatter",
                    "title": f"Scatter Plot: {numerical_cols[0]} vs {numerical_cols[1]}",
                    "description": f"Shows relationship between {numerical_cols[0]} and {numerical_cols[1]}",
                    "x_column": numerical_cols[0],
                    "y_column": numerical_cols[1]
                })
            
            # Correlation heatmap
            if len(numerical_cols) >= 3:
                suggestions.append({
                    "type": "heatmap",
                    "title": "Correlation Heatmap",
                    "description": "Shows correlations between all numerical columns",
                    "columns": list(numerical_cols)
                })
            
            return suggestions
            
        except Exception as e:
            self.observability.log_error(f"Visualization suggestions failed: {str(e)}")
            return []


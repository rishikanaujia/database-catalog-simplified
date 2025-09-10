"""UI generation tool for data catalog interface"""

import pandas as pd
import gradio as gr
from typing import Dict, Any, Tuple
from src.core.config import logger

class UIGenerator:
    """Generates interactive web interface for data catalog"""
    
    def __init__(self, documented_df: pd.DataFrame):
        self.df = documented_df
        self.tables = sorted(self.df['table_name'].unique())
        self.business_types = sorted(self.df['business_data_type'].unique())
    
    def create_interface(self) -> str:
        """Create and launch Gradio interface"""
        logger.info("Creating Gradio interface for data catalog")
        
        def search_catalog(search_term: str, table_filter: str, type_filter: str) -> Tuple[str, str]:
            """Search the data catalog"""
            filtered_df = self.df.copy()
            
            # Apply filters
            if table_filter != "All Tables":
                filtered_df = filtered_df[filtered_df['table_name'] == table_filter]
            
            if type_filter != "All Types":
                filtered_df = filtered_df[filtered_df['business_data_type'] == type_filter]
            
            # Apply search
            if search_term:
                search_mask = (
                    filtered_df['table_name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['column_name'].str.contains(search_term, case=False, na=False) |
                    filtered_df['column_description'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[search_mask]
            
            if len(filtered_df) == 0:
                return "No results found.", ""
            
            # Create results HTML
            results_html = self._format_results(filtered_df.head(50))
            summary = f"Found {len(filtered_df)} results across {filtered_df['table_name'].nunique()} tables"
            
            return summary, results_html
        
        def get_table_details(table_name: str) -> str:
            """Get detailed table information"""
            if table_name == "Select a table":
                return "Please select a table to view details."
            
            table_df = self.df[self.df['table_name'] == table_name]
            if len(table_df) == 0:
                return "Table not found."
            
            # Get table description
            table_desc = table_df.iloc[0].get('table_description', 'No description available.')
            
            # Create table details HTML
            details_html = f"""
            <h2>{table_name}</h2>
            <p><strong>Description:</strong> {table_desc}</p>
            <p><strong>Columns:</strong> {len(table_df)}</p>
            
            <table style="width:100%; border-collapse: collapse;">
                <tr style="background-color: #f2f2f2;">
                    <th style="border: 1px solid #ddd; padding: 8px;">Column</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Type</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Role</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Description</th>
                </tr>
            """
            
            for _, col in table_df.iterrows():
                details_html += f"""
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px;"><code>{col['column_name']}</code></td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{col['data_type']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{col['column_role']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{col.get('column_description', '')}</td>
                </tr>
                """
            
            details_html += "</table>"
            return details_html
        
        # Create Gradio interface
        with gr.Blocks(title="Database Catalog", theme=gr.themes.Soft()) as interface:
            gr.Markdown(f"""
            # Database Catalog
            Interactive catalog for {len(self.tables)} tables and {len(self.df)} columns
            """)
            
            with gr.Tabs():
                with gr.Tab("Search Catalog"):
                    with gr.Row():
                        search_input = gr.Textbox(label="Search", placeholder="Search tables, columns, or descriptions...")
                        table_filter = gr.Dropdown(choices=["All Tables"] + self.tables, label="Table", value="All Tables")
                        type_filter = gr.Dropdown(choices=["All Types"] + self.business_types, label="Business Type", value="All Types")
                    
                    search_btn = gr.Button("Search", variant="primary")
                    search_summary = gr.Markdown()
                    search_results = gr.HTML()
                    
                    search_btn.click(
                        search_catalog,
                        inputs=[search_input, table_filter, type_filter],
                        outputs=[search_summary, search_results]
                    )
                
                with gr.Tab("Browse Tables"):
                    table_selector = gr.Dropdown(choices=["Select a table"] + self.tables, label="Choose Table", value="Select a table")
                    table_details = gr.HTML()
                    
                    table_selector.change(
                        get_table_details,
                        inputs=[table_selector],
                        outputs=[table_details]
                    )
        
        # Launch interface
        url = interface.launch(server_name="0.0.0.0", server_port=7860, share=False, inbrowser=True)
        return "http://localhost:7860"
    
    def _format_results(self, df: pd.DataFrame) -> str:
        """Format search results as HTML table"""
        html = """
        <table style="width:100%; border-collapse: collapse;">
            <tr style="background-color: #f2f2f2;">
                <th style="border: 1px solid #ddd; padding: 8px;">Table</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Column</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Type</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Business Type</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Description</th>
            </tr>
        """
        
        for _, row in df.iterrows():
            description = str(row.get('column_description', ''))[:100] + "..." if len(str(row.get('column_description', ''))) > 100 else str(row.get('column_description', ''))
            
            html += f"""
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>{row['table_name']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;"><code>{row['column_name']}</code></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{row['data_type']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{row['business_data_type']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{description}</td>
            </tr>
            """
        
        html += "</table>"
        return html

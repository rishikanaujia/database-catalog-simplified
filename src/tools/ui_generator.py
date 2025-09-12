"""UI generation tool for data catalog interface - Updated with configuration"""

import pandas as pd
import gradio as gr
from typing import Dict, Any, Tuple
from src.core.config import logger

# NEW: Import the UI config
from src.core.ui_config import UI_CONFIG

class UIGenerator:
    """Generates interactive web interface for data catalog"""
    
    def __init__(self, documented_df: pd.DataFrame):
        self.df = documented_df
        self.tables = sorted(self.df['table_name'].unique())
        self.business_types = sorted(self.df['business_data_type'].unique())
    
    def create_interface(self) -> str:
        """Create and launch Gradio interface - UPDATED to use configuration"""
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
            
            # Use configurable results limit
            max_results = min(len(filtered_df), UI_CONFIG.max_search_results)
            display_df = filtered_df.head(max_results)
            
            # Create results HTML with config-based styling
            results_html = self._format_results(display_df)
            
            # Enhanced summary with configuration
            summary_parts = [f"Found {len(filtered_df)} results across {filtered_df['table_name'].nunique()} tables"]
            if len(filtered_df) > max_results:
                summary_parts.append(f"(showing first {max_results} results)")
            
            summary = " ".join(summary_parts)
            
            return summary, results_html
        
        def get_table_details(table_name: str) -> str:
            """Get detailed table information - UPDATED with config styling"""
            if table_name == "Select a table":
                return "Please select a table to view details."
            
            table_df = self.df[self.df['table_name'] == table_name]
            if len(table_df) == 0:
                return "Table not found."
            
            # Get table description
            table_desc = table_df.iloc[0].get('table_description', 'No description available.')
            
            # Create table details HTML with configurable styling
            details_html = f"""
            <div style="font-family: {UI_CONFIG.font_family}; font-size: {UI_CONFIG.font_size};">
                <h2 style="color: {UI_CONFIG.primary_color};">{table_name}</h2>
                <p><strong>Description:</strong> {table_desc}</p>
                <p><strong>Columns:</strong> {len(table_df)}</p>
                
                <table style="{UI_CONFIG.get_table_style()}">
                    <tr>
                        <th style="{UI_CONFIG.get_header_style()}">Column</th>
                        <th style="{UI_CONFIG.get_header_style()}">Type</th>
                        <th style="{UI_CONFIG.get_header_style()}">Role</th>
                        <th style="{UI_CONFIG.get_header_style()}">Description</th>
                    </tr>
            """
            
            for i, (_, col) in enumerate(table_df.iterrows()):
                # Alternate row colors using config
                if i % 2 == 0:
                    row_bg = UI_CONFIG._config['styling']['even_row_background']
                else:
                    row_bg = UI_CONFIG._config['styling']['odd_row_background']
                
                # Truncate description using config
                description = UI_CONFIG.truncate_text(
                    str(col.get('column_description', '')), 
                    is_description=True
                )
                
                details_html += f"""
                <tr style="background-color: {row_bg};" onmouseover="this.style.backgroundColor='{UI_CONFIG._config['styling']['hover_background']}'" onmouseout="this.style.backgroundColor='{row_bg}'">
                    <td style="{UI_CONFIG.get_cell_style(is_code=True)}"><code>{col['column_name']}</code></td>
                    <td style="{UI_CONFIG.get_cell_style()}">{col['data_type']}</td>
                    <td style="{UI_CONFIG.get_cell_style()}">{col['column_role']}</td>
                    <td style="{UI_CONFIG.get_cell_style()}">{description}</td>
                </tr>
                """
            
            details_html += """
                </table>
            </div>
            """
            return details_html
        
        # Create Gradio interface with configurable theme
        theme_config = UI_CONFIG.get_gradio_theme_config()
        
        with gr.Blocks(
            title="Database Catalog", 
            theme=gr.themes.Soft(
                primary_hue=theme_config['primary_hue'],
                secondary_hue=theme_config['secondary_hue'],
                neutral_hue=theme_config['neutral_hue']
            )
        ) as interface:
            
            # Header with configurable styling
            gr.Markdown(f"""
            # Database Catalog
            <div style="color: {UI_CONFIG.primary_color}; font-family: {UI_CONFIG.font_family};">
                Interactive catalog for <strong>{len(self.tables)} tables</strong> and <strong>{len(self.df)} columns</strong>
            </div>
            """)
            
            with gr.Tabs():
                # Search tab (using config for default tab)
                with gr.Tab("Search Catalog"):
                    with gr.Row():
                        search_input = gr.Textbox(
                            label="Search", 
                            placeholder=UI_CONFIG.search_placeholder
                        )
                        table_filter = gr.Dropdown(
                            choices=["All Tables"] + self.tables, 
                            label="Table", 
                            value="All Tables"
                        )
                        type_filter = gr.Dropdown(
                            choices=["All Types"] + self.business_types, 
                            label="Business Type", 
                            value="All Types"
                        )
                    
                    search_btn = gr.Button("Search", variant="primary")
                    search_summary = gr.Markdown()
                    search_results = gr.HTML()
                    
                    # Optional: Add export button if enabled
                    if UI_CONFIG.enable_csv_export:
                        export_btn = gr.Button("Export Results", variant="secondary")
                    
                    search_btn.click(
                        search_catalog,
                        inputs=[search_input, table_filter, type_filter],
                        outputs=[search_summary, search_results]
                    )
                
                # Browse tab
                with gr.Tab("Browse Tables"):
                    table_selector = gr.Dropdown(
                        choices=["Select a table"] + self.tables, 
                        label="Choose Table", 
                        value="Select a table"
                    )
                    table_details = gr.HTML()
                    
                    table_selector.change(
                        get_table_details,
                        inputs=[table_selector],
                        outputs=[table_details]
                    )
        
        # Launch interface with configurable server settings
        logger.info(f"Launching UI on {UI_CONFIG.host}:{UI_CONFIG.port}")
        url = interface.launch(
            server_name=UI_CONFIG.host,
            server_port=UI_CONFIG.port,
            share=UI_CONFIG.share,
            inbrowser=UI_CONFIG.inbrowser,
            max_threads=UI_CONFIG.max_threads,
            show_error=UI_CONFIG.show_error,
            show_api=UI_CONFIG.show_api
        )
        
        return f"http://localhost:{UI_CONFIG.port}"
    
    def _format_results(self, df: pd.DataFrame) -> str:
        """Format search results as HTML table - UPDATED with configurable styling"""
        
        # Use configurable results per page
        display_df = df.head(UI_CONFIG.results_per_page)
        
        html = f"""
        <div style="font-family: {UI_CONFIG.font_family};">
            <table style="{UI_CONFIG.get_table_style()}">
                <tr>
                    <th style="{UI_CONFIG.get_header_style()}">Table</th>
                    <th style="{UI_CONFIG.get_header_style()}">Column</th>
                    <th style="{UI_CONFIG.get_header_style()}">Type</th>
                    <th style="{UI_CONFIG.get_header_style()}">Business Type</th>
                    <th style="{UI_CONFIG.get_header_style()}">Description</th>
                </tr>
        """
        
        for i, (_, row) in enumerate(display_df.iterrows()):
            # Alternate row colors using config
            if i % 2 == 0:
                row_bg = UI_CONFIG._config['styling']['even_row_background']
            else:
                row_bg = UI_CONFIG._config['styling']['odd_row_background']
            
            # Truncate description using config
            description = UI_CONFIG.truncate_text(
                str(row.get('column_description', '')), 
                is_description=True
            )
            
            html += f"""
            <tr style="background-color: {row_bg};" onmouseover="this.style.backgroundColor='{UI_CONFIG._config['styling']['hover_background']}'" onmouseout="this.style.backgroundColor='{row_bg}'">
                <td style="{UI_CONFIG.get_cell_style()}"><strong>{row['table_name']}</strong></td>
                <td style="{UI_CONFIG.get_cell_style(is_code=True)}"><code>{row['column_name']}</code></td>
                <td style="{UI_CONFIG.get_cell_style()}">{row['data_type']}</td>
                <td style="{UI_CONFIG.get_cell_style()}">{row['business_data_type']}</td>
                <td style="{UI_CONFIG.get_cell_style()}">{description}</td>
            </tr>
            """
        
        html += """
            </table>
        </div>
        """
        
        # Add pagination info if results were truncated
        if len(df) > UI_CONFIG.results_per_page:
            html += f"""
            <p style="color: {UI_CONFIG._config['theme']['warning_color']}; font-style: italic; margin-top: 10px;">
                Showing {UI_CONFIG.results_per_page} of {len(df)} results
            </p>
            """
        
        return html
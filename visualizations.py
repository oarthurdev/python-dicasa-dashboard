import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_heatmap(activities_df):
    """
    Create a heatmap of activities by day of week and hour
    
    Args:
        activities_df (pd.DataFrame): DataFrame containing activity data
    
    Returns:
        plotly.graph_objects.Figure: Heatmap figure
    """
    try:
        logger.info("Creating activity heatmap")
        
        if activities_df.empty or 'dia_semana' not in activities_df.columns or 'hora' not in activities_df.columns:
            logger.warning("Insufficient data for heatmap creation")
            # Return empty figure
            fig = go.Figure()
            fig.update_layout(
                title="Mapa de Calor de Atividades",
                xaxis_title="Dia da Semana",
                yaxis_title="Hora do Dia",
                height=400,
            )
            return fig
        
        # Order days of week correctly
        day_order = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        
        # Count activities by day and hour
        heatmap_data = activities_df.groupby(['dia_semana', 'hora']).size().reset_index(name='count')
        
        # Create a complete grid with all days and hours
        all_days = pd.DataFrame({'dia_semana': day_order})
        all_hours = pd.DataFrame({'hora': range(24)})
        grid = all_days.merge(all_hours, how='cross')
        
        # Merge with actual data
        heatmap_data = grid.merge(heatmap_data, on=['dia_semana', 'hora'], how='left')
        heatmap_data['count'] = heatmap_data['count'].fillna(0)
        
        # Create heatmap
        fig = px.density_heatmap(
            heatmap_data,
            x='dia_semana',
            y='hora',
            z='count',
            category_orders={'dia_semana': day_order, 'hora': list(range(24))},
            labels={'count': 'Atividades', 'dia_semana': 'Dia da Semana', 'hora': 'Hora do Dia'},
            color_continuous_scale='Blues',
        )
        
        # Update layout
        fig.update_layout(
            title="Mapa de Calor de Atividades",
            xaxis_title="Dia da Semana",
            yaxis_title="Hora do Dia",
            height=400,
            coloraxis_colorbar=dict(title="Atividades"),
        )
        
        # Update y-axis to show all hours
        fig.update_yaxes(tickvals=list(range(24)))
        
        return fig
    
    except Exception as e:
        logger.error(f"Error creating heatmap: {str(e)}")
        # Return fallback empty figure
        fig = go.Figure()
        fig.update_layout(
            title="Mapa de Calor de Atividades (Erro)",
            height=400,
        )
        fig.add_annotation(
            text=f"Erro ao criar mapa de calor: {str(e)}",
            showarrow=False,
            font=dict(size=14, color="red")
        )
        return fig

def create_conversion_funnel(leads_df):
    """
    Create a conversion funnel visualization
    
    Args:
        leads_df (pd.DataFrame): DataFrame containing lead data
    
    Returns:
        plotly.graph_objects.Figure: Funnel figure
    """
    try:
        logger.info("Creating conversion funnel")
        
        if leads_df.empty or 'etapa_categoria' not in leads_df.columns:
            logger.warning("Insufficient data for funnel creation")
            # Return empty figure
            fig = go.Figure()
            fig.update_layout(
                title="Funil de Conversão",
                height=400,
            )
            return fig
        
        # Define funnel stages in order
        funnel_stages = ['Contato Inicial', 'Visita', 'Proposta', 'Venda']
        
        # Count leads in each stage
        stage_counts = leads_df['etapa_categoria'].value_counts().reindex(funnel_stages, fill_value=0)
        
        # Create funnel chart
        fig = go.Figure(go.Funnel(
            y=stage_counts.index,
            x=stage_counts.values,
            textinfo="value+percent initial",
            marker=dict(color=[
                "#2E86C1",  # Blue for initial contact
                "#F39C12",  # Orange for visit
                "#16A085",  # Green for proposal
                "#8E44AD",  # Purple for sale
            ]),
        ))
        
        # Update layout
        fig.update_layout(
            title="Funil de Conversão",
            height=400,
            margin=dict(l=50, r=50, t=80, b=20),
        )
        
        return fig
    
    except Exception as e:
        logger.error(f"Error creating conversion funnel: {str(e)}")
        # Return fallback empty figure
        fig = go.Figure()
        fig.update_layout(
            title="Funil de Conversão (Erro)",
            height=400,
        )
        fig.add_annotation(
            text=f"Erro ao criar funil: {str(e)}",
            showarrow=False,
            font=dict(size=14, color="red")
        )
        return fig

def create_points_breakdown_chart(broker_data):
    """
    Create a bar chart showing the breakdown of points by category
    
    Args:
        broker_data (pd.Series): Series containing broker points data
    
    Returns:
        plotly.graph_objects.Figure: Bar chart figure
    """
    try:
        logger.info("Creating points breakdown chart")
        
        # Define point categories and their values
        categories = {
            'Leads respondidos em 1h': ('leads_respondidos_1h', 2),
            'Leads visitados': ('leads_visitados', 5),
            'Propostas enviadas': ('propostas_enviadas', 8),
            'Vendas realizadas': ('vendas_realizadas', 15),
            'Leads atualizados no mesmo dia': ('leads_atualizados_mesmo_dia', 2),
            'Leads sem interação por 24h': ('leads_sem_interacao_24h', -3)
        }
        
        # Calculate points for each category
        points_breakdown = []
        
        for category_name, (column, points_per_item) in categories.items():
            if column in broker_data:
                count = broker_data[column]
                total_points = count * points_per_item
                points_breakdown.append({
                    'categoria': category_name,
                    'quantidade': count,
                    'pontos': total_points,
                    'tipo': 'Positivo' if points_per_item > 0 else 'Negativo'
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(points_breakdown)
        
        if df.empty:
            # Return empty figure
            fig = go.Figure()
            fig.update_layout(
                title="Detalhamento de Pontos",
                height=400,
            )
            return fig
        
        # Sort by absolute value of points
        df = df.sort_values('pontos', key=abs, ascending=False)
        
        # Create bar chart
        fig = px.bar(
            df,
            x='categoria',
            y='pontos',
            color='tipo',
            text='quantidade',
            labels={'categoria': 'Categoria', 'pontos': 'Pontos', 'quantidade': 'Quantidade'},
            color_discrete_map={'Positivo': '#28A745', 'Negativo': '#DC3545'},
            height=400,
        )
        
        # Update layout
        fig.update_layout(
            title="Detalhamento de Pontos",
            xaxis_title="",
            yaxis_title="Pontos",
            legend_title="Tipo",
        )
        
        # Show counts on bars
        fig.update_traces(texttemplate='%{text}', textposition='auto')
        
        return fig
    
    except Exception as e:
        logger.error(f"Error creating points breakdown chart: {str(e)}")
        # Return fallback empty figure
        fig = go.Figure()
        fig.update_layout(
            title="Detalhamento de Pontos (Erro)",
            height=400,
        )
        fig.add_annotation(
            text=f"Erro ao criar gráfico: {str(e)}",
            showarrow=False,
            font=dict(size=14, color="red")
        )
        return fig

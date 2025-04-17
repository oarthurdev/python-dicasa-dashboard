import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_heatmap(activities_df, activity_type=None, lead_filter=None):
    """
    Create an enhanced heatmap of activities by day of week and hour with filtering options
    Now fully aligned with streamlit dashboard integration

    Args:
        activities_df (pd.DataFrame): DataFrame containing activity data
        activity_type (str, optional): Filter for specific activity type (e.g., 'mensagem_enviada')
        lead_filter (dict, optional): Future support for filtering by leads

    Returns:
        plotly.graph_objects.Figure: Heatmap chart
    """
    try:
        logger.info("[HEATMAP] Iniciando criação do mapa de calor")

        if activities_df.empty or 'dia_semana' not in activities_df.columns or 'hora' not in activities_df.columns:
            logger.warning("[HEATMAP] Dados insuficientes para criar heatmap")
            return go.Figure()

        filtered = activities_df.copy()

        if activity_type:
            filtered = filtered[filtered['tipo'] == activity_type]

        filtered = filtered[(filtered['hora'] >= 8) & (filtered['hora'] <= 21)]

        if filtered.empty:
            logger.warning("[HEATMAP] Nenhum dado após os filtros")
            return go.Figure()

        # Traduz dia da semana do inglês para português
        dias_traducao = {
            'MONDAY': 'Segunda',
            'TUESDAY': 'Terça',
            'WEDNESDAY': 'Quarta',
            'THURSDAY': 'Quinta',
            'FRIDAY': 'Sexta',
            'SATURDAY': 'Sábado',
            'SUNDAY': 'Domingo',
            'Monday': 'Segunda',
            'Tuesday': 'Terça',
            'Wednesday': 'Quarta',
            'Thursday': 'Quinta',
            'Friday': 'Sexta',
            'Saturday': 'Sábado',
            'Sunday': 'Domingo'
        }

        # Define as categorias e ordem
        day_order = [
            'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado',
            'Domingo'
        ]

        # Garante que criado_em é datetime
        filtered['criado_em'] = pd.to_datetime(filtered['criado_em'])

        # Converte dia_semana para string e uppercase
        filtered['dia_semana'] = filtered['dia_semana'].astype(
            str).str.strip().str.upper()

        # Se dia_semana for NaN ou 'NAN', usa criado_em
        filtered.loc[filtered['dia_semana'].isin(['NAN', 'NAN ']),
                     'dia_semana'] = filtered['criado_em'].dt.strftime(
                         '%A').str.upper()

        # Traduz dias para português
        filtered['dia_semana'] = filtered['dia_semana'].map(dias_traducao)

        # Garante que todos os dias estejam na lista de categorias antes de converter
        filtered.loc[~filtered['dia_semana'].isin(day_order),
                     'dia_semana'] = day_order[0]

        # Converte para categoria após garantir valores válidos
        filtered['dia_semana'] = pd.Categorical(filtered['dia_semana'],
                                                categories=day_order,
                                                ordered=True)

        time_blocks = {
            '08h - 10h': [8, 9],
            '10h - 12h': [10, 11],
            '12h - 14h': [12, 13],
            '14h - 16h': [14, 15],
            '16h - 18h': [16, 17],
            '18h - 20h': [18, 19],
            '20h - 22h': [20, 21],
        }

        hour_to_block = {
            hour: block
            for block, hours in time_blocks.items()
            for hour in hours
        }
        filtered['time_block'] = filtered['hora'].map(hour_to_block)

        heatmap_data = filtered.groupby(
            ['dia_semana', 'time_block'],
            observed=True).size().reset_index(name='count')

        all_days = pd.DataFrame({'dia_semana': day_order})
        all_blocks = pd.DataFrame({'time_block': list(time_blocks.keys())})
        grid = all_days.merge(all_blocks, how='cross')
        heatmap_data = grid.merge(heatmap_data,
                                  on=['dia_semana', 'time_block'],
                                  how='left')
        heatmap_data['count'] = heatmap_data['count'].fillna(0)

        logger.info("[HEATMAP] Grid completo gerado com contagens preenchidas")

        fig = go.Figure()
        fig.add_trace(
            go.Heatmap(
                x=heatmap_data['dia_semana'],
                y=heatmap_data['time_block'],
                z=heatmap_data['count'],
                colorscale='Blues',
                hoverongaps=False,
                hovertemplate=
                'Dia: %{x}<br>Horário: %{y}<br>Atividades: %{z}<extra></extra>'
            ))

        fig.update_layout(xaxis={
            'title': "Dia da Semana",
            'categoryorder': 'array',
            'categoryarray': day_order
        },
                          yaxis={
                              'title': "Faixa de Horário",
                              'categoryorder': 'array',
                              'categoryarray': list(time_blocks.keys())
                          },
                          height=400,
                          margin=dict(l=60, r=60, t=80, b=60),
                          paper_bgcolor='rgba(0,0,0,0)',
                          plot_bgcolor='rgba(0,0,0,0)')

        logger.info("[HEATMAP] Heatmap finalizado com sucesso")
        return fig

    except Exception as e:
        logger.error(f"[HEATMAP] Erro ao criar heatmap: {str(e)}")
        fig = go.Figure()
        fig.update_layout(title="Erro ao criar Mapa de Calor")
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

        if leads_df.empty or 'etapa' not in leads_df.columns:
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
        stage_counts = leads_df['etapa'].value_counts().reindex(
            funnel_stages, fill_value=0)

        # Create funnel chart
        fig = go.Figure(
            go.Funnel(
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
        fig.add_annotation(text=f"Erro ao criar funil: {str(e)}",
                           showarrow=False,
                           font=dict(size=14, color="red"))
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

        # Define point categories and their values with expanded gamification rules
        categories = {
            # Positive categories
            'Leads respondidos em 1h': ('leads_respondidos_1h', 2),
            'Leads visitados': ('leads_visitados', 5),
            'Propostas enviadas': ('propostas_enviadas', 8),
            'Vendas realizadas': ('vendas_realizadas', 15),
            'Leads atualizados no mesmo dia':
            ('leads_atualizados_mesmo_dia', 2),
            'Feedbacks positivos': ('feedbacks_positivos', 3),
            'Resposta rápida (3h)': ('resposta_rapida_3h', 4),
            'Todos leads respondidos': ('todos_leads_respondidos', 5),
            'Cadastros completos': ('cadastro_completo', 3),
            'Acompanhamento pós-venda': ('acompanhamento_pos_venda', 10),

            # Negative categories
            'Leads sem interação (24h)': ('leads_sem_interacao_24h', -3),
            'Leads ignorados (48h)': ('leads_ignorados_48h', -5),
            'Leads com reclamação': ('leads_com_reclamacao', -4),
            'Leads perdidos': ('leads_perdidos', -6)
        }

        # Calculate points for each category
        points_breakdown = []

        for category_name, (column, points_per_item) in categories.items():
            if column in broker_data and (not pd.isna(broker_data[column])
                                          and broker_data[column] > 0):
                count = broker_data[column]
                total_points = count * points_per_item
                points_breakdown.append({
                    'categoria':
                    category_name,
                    'quantidade':
                    int(count),
                    'pontos':
                    int(total_points),
                    'tipo':
                    'Positivo' if points_per_item > 0 else 'Negativo'
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

        # Calculate totals for annotation
        total_positive = df[df['tipo'] == 'Positivo']['pontos'].sum()
        total_negative = abs(df[df['tipo'] == 'Negativo']['pontos'].sum())

        # Create bar chart with enhanced styling
        fig = px.bar(
            df,
            x='categoria',
            y='pontos',
            color='tipo',
            text='quantidade',
            labels={
                'categoria': 'Categoria',
                'pontos': 'Pontos',
                'quantidade': 'Quantidade'
            },
            color_discrete_map={
                'Positivo': '#28A745',
                'Negativo': '#DC3545'
            },
            height=400,
        )

        # Update layout
        fig.update_layout(
            title={
                'text': "Detalhamento de Pontos",
                'font': {
                    'size': 18,
                    'color': '#1E3A8A'
                },
                'y': 0.95
            },
            xaxis={
                'title': "",
                'tickangle': -45,
                'tickfont': {
                    'size': 10
                },
                'gridcolor': 'rgba(0,0,0,0.1)'
            },
            yaxis={
                'title': "Pontos",
                'tickfont': {
                    'size': 12
                },
                'gridcolor': 'rgba(0,0,0,0.1)'
            },
            legend={
                'title': "Tipo",
                'orientation': 'h',
                'y': 1.1,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'bottom'
            },
            margin=dict(l=50, r=50, t=100, b=100),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
        )

        # Show counts on bars
        fig.update_traces(texttemplate='%{text}', textposition='auto')

        # Add summary annotation
        fig.add_annotation(
            x=0.5,
            y=-0.15,
            xref="paper",
            yref="paper",
            text=
            f"Pontos positivos: {total_positive} | Pontos negativos: {total_negative} | Balanço: {total_positive - total_negative}",
            showarrow=False,
            font=dict(size=12),
            align="center",
            bgcolor="rgba(245, 245, 245, 0.8)",
            borderpad=4)

        # Add explanatory annotation for different categories
        if len(df) > 5:  # Only add explanation if we have enough data points
            # Find the best performing category
            best_category = df[df['tipo'] == 'Positivo'].sort_values(
                'pontos', ascending=False
            ).iloc[0] if not df[df['tipo'] == 'Positivo'].empty else None

            # Find the worst performing category
            worst_category = df[df['tipo'] == 'Negativo'].sort_values(
                'pontos'
            ).iloc[0] if not df[df['tipo'] == 'Negativo'].empty else None

            if best_category is not None:
                fig.add_annotation(x=best_category['categoria'],
                                   y=best_category['pontos'],
                                   text="Melhor desempenho",
                                   showarrow=True,
                                   arrowhead=2,
                                   arrowsize=1,
                                   arrowwidth=2,
                                   arrowcolor="#28A745",
                                   font=dict(size=10, color="#28A745"),
                                   align="center",
                                   borderpad=4,
                                   yshift=15)

            if worst_category is not None:
                fig.add_annotation(x=worst_category['categoria'],
                                   y=worst_category['pontos'],
                                   text="Oportunidade de melhoria",
                                   showarrow=True,
                                   arrowhead=2,
                                   arrowsize=1,
                                   arrowwidth=2,
                                   arrowcolor="#DC3545",
                                   font=dict(size=10, color="#DC3545"),
                                   align="center",
                                   borderpad=4,
                                   yshift=-15)

        return fig

    except Exception as e:
        logger.error(f"Error creating points breakdown chart: {str(e)}")
        # Return fallback empty figure
        fig = go.Figure()
        fig.update_layout(
            title="Detalhamento de Pontos (Erro)",
            height=400,
        )
        fig.add_annotation(text=f"Erro ao criar gráfico: {str(e)}",
                           showarrow=False,
                           font=dict(size=14, color="red"))
        return fig

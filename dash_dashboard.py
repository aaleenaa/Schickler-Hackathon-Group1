from cProfile import label
import pandas as pd
import plotly.express as px  # (version 4.7.0 or higher)
import dash
from dash import Dash, dcc, html, Input, Output  # pip install dash (version 2.0.0 or higher)


data = pd.read_parquet("aggregate_data_98_percentile_yearmonth.parquet")
sorted_topics = sorted(pd.unique(data.topic))

app = dash.Dash(__name__)

app.layout = html.Div(
    children=[
        html.H1(children="Schickler Hackathon – Group 1",
        style={"fontSize": "48px", "color": "red"},
               ),
        html.P(
            children="Topic analysis based on articles using view counts data",        ),
    dcc.Dropdown(id="slct_year",
                 options=[
                     {"label": "2017", "value": 2017},
                     {"label": "2018", "value": 2018},
                     {"label": "2019", "value": 2019},
                     {"label": "2020", "value": 2020},
                     
                     ],
                 multi=False,
                 value=2020,
                 style={'width': "40%"}
                 ),
    dcc.Dropdown(id="slct_topic",
                options=[{"label": listitem, "value": listitem} for listitem in sorted_topics],
                multi=True,
                value=[sorted_topics[4], sorted_topics[8], sorted_topics[9]],
                style={'width': "100%"}
                ),
    html.Div(id='output_container', children=[]),
    dcc.Graph(id='views_absolute'),
    dcc.Graph(id='views_relative'),
    dcc.Graph(id="category_comparison")
    ]
)


@app.callback(
    [Output(component_id='output_container', component_property='children'),
     Output(component_id='views_absolute', component_property='figure'),
     Output(component_id='views_relative', component_property='figure'),
     Output(component_id='category_comparison', component_property='figure'),
    ],
    [Input(component_id='slct_year', component_property='value'),
    Input(component_id="slct_topic", component_property="value")]
)
def update_graphs(year_slct, topic_slct):
    print(year_slct)
    print(type(year_slct))

    container = ""

    
    dff = data.copy()
    filter_df = dff[(dff["published_yearmonth"] >= f"{year_slct}") & (dff["published_yearmonth"] < f"{year_slct+1}")]
    filter_df = filter_df[filter_df["topic"].isin(topic_slct)]
    groupby_filter_df = filter_df.groupby(["published_yearmonth", "topic"]).sum().reset_index()
    fig_absolute = px.bar(groupby_filter_df.dropna(), x="published_yearmonth", y="count_views", color="topic", labels={"published_yearmonth": f"Month of {year_slct}", "count_views": "Absolute Views"})


    totalviews_yearmonth = groupby_filter_df.groupby(["published_yearmonth"]).sum().reset_index()[["published_yearmonth","count_views"]]
    totalviews_yearmonth = totalviews_yearmonth.rename(columns={"count_views": "count_totals"})

    groupby_filter_df_relative_views = groupby_filter_df.groupby(["published_yearmonth", "topic"]).sum().reset_index().merge(totalviews_yearmonth, on="published_yearmonth")
    groupby_filter_df_relative_views["count_relative"] = groupby_filter_df_relative_views["count_views"]/groupby_filter_df_relative_views["count_totals"]*100

    fig_relative = px.bar(groupby_filter_df_relative_views.dropna(), x="published_yearmonth", y="count_relative", color="topic", labels={"published_yearmonth": f"Published in Month of {year_slct}", "count_relative": "Relative Views"})



    data_98_percentile_year_agg = groupby_filter_df.copy()
    data_98_percentile_year_agg["published_year"] = data_98_percentile_year_agg.published_yearmonth.map(lambda x: x.year)
    
    totalviews_year = data_98_percentile_year_agg.groupby(["published_year"]).sum()
    columns = totalviews_year.columns
    columns_new_dict = {column: f"{column}_sum" for column in columns}
    totalviews_year = totalviews_year.reset_index()
    totalviews_year = totalviews_year.rename(columns=columns_new_dict)

    data_98_percentile_year_agg = data_98_percentile_year_agg.groupby(["published_year", "published_yearmonth", "topic"]).sum().reset_index().merge(totalviews_year, on="published_year")

    data_98_percentile_year_agg["<5"] / data_98_percentile_year_agg["<5_sum"] * 100

    for column in columns:
        data_98_percentile_year_agg[f"{column}_rel"] = data_98_percentile_year_agg[column] / data_98_percentile_year_agg[f"{column}_sum"] * 100

    melted_df = data_98_percentile_year_agg[["topic", "<5_rel", "<30_rel", "<60_rel", ">60_rel"]].melt(id_vars="topic")
    print(pd.unique(melted_df["variable"]))

    # fig_categories = px.bar(melted_df, x="topic", y="value", color="variable")
    fig_categories = px.bar(melted_df, x="topic", y="value", color="variable")

    return container, fig_absolute, fig_relative, fig_categories




if __name__ == "__main__":
    app.run_server(debug=True)





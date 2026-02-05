# Databricks notebook source
pip install jinja2

# COMMAND ----------

parameters=[
    {
        "table":"spotify_cata.silver.factstream",
        "alias":"factstream",
        "cols":"factstream.stream_id,factstream.listen_duration"
    },
    {
        "table":"spotify_cata.silver.dimuser",
        "alias":"dimuser",
        "cols":"dimuser.user_id,dimuser.user_name",
        "condition":"factstream.user_id = dimuser.user_id"
    },
    {
        "table":"spotify_cata.silver.dimtrack",
        "alias":"dimtrack",
        "cols":"dimtrack.track_id,dimtrack.track_name",
        "condition":"factstream.track_id = dimtrack.track_id"
    }
]

# COMMAND ----------

from jinja2 import Template
query_text="""
    select
    {% for param in parameter %}
        {{ param.cols }}
        {% if not loop.last %} 
            ,
        {% endif %}
    {% endfor %}
    from
    {% for param in parameter %}
        {% if loop.first %}
            {{ param.table }} as {{ param.alias }}
        {% endif %}
    {% endfor %}
    {% for param in parameter %}
        {% if not loop.first %}
            left join
        
            {{ param.table }} as {{ param.alias }}
            on
            {{ param.condition }}
        {% endif %}
    {% endfor %}

"""

# COMMAND ----------

jinja_sql_str=Template(query_text)  # Ensure all 'for' blocks in query_text are properly closed with 'endfor'
query = jinja_sql_str.render(parameter=parameters)
print(query)

# COMMAND ----------

spark.sql(query)

# COMMAND ----------


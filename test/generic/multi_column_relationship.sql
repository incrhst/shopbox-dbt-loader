{% test multi_column_relationship(model, to, from_columns, to_columns) %}

select count(*) as validation_errors
from (
    select distinct {% for col in from_columns %} {{col}} {% if not loop.last %},{% endif %} {% endfor %}
    from {{ model }} source
    where not exists (
        select 1 
        from {{ to }} target
        where {% for i in range(from_columns|length) %}
            target.{{ to_columns[i] }} = source.{{ from_columns[i] }}
            {% if not loop.last %}and{% endif %}
        {% endfor %}
    )
    and {% for col in from_columns %} 
        source.{{col}} is not null
        {% if not loop.last %}and{% endif %}
    {% endfor %}
) validation_errors

{% endtest %}

{#- Fails on the rows where the boolean SQL expression is false -#}
{% test expression_is_true(model, expression, column_name=none) %}
    select * from {{ model }} where not ({{ expression }})
{% endtest %}

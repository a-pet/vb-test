
{%- macro get_max_value_from_json(actions_field, action_type_value, data_type='INT64') -%}
    array(
        select max(value)
        from 
            UNNEST(json_extract_array({{ actions_field }})) as action_json,
            UNNEST([json_extract_scalar(action_json, '$.action_type')]) as action_type,
            UNNEST([cast(json_extract_scalar(action_json, '$.value') as {{data_type}})]) as value
        where action_type = '{{ action_type_value }}'
    )[safe_offset(0)]
{%- endmacro -%}

{%- macro get_sum_value_from_json(actions_field, json_path) -%}
    array(
        select
            sum(
                cast(
                    JSON_EXTRACT_SCALAR(
                        unnest_action, 
                        '{{ json_path }}'
                    ) as INT64
                )
            ) as result
        from 
            UNNEST(
                JSON_EXTRACT_ARRAY(
                    {{ actions_field }}
                )
            ) as unnest_action
    )[safe_offset(0)]
{%- endmacro -%}




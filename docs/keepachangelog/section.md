### {{ section.type or "Misc" }}
{% for commit in section.commits|sort(attribute='author_date',reverse=true)|unique(attribute='subject') -%}
{% include 'commit.md' with context %}
{% endfor %}

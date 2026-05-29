{% for section, _ in sections.items() %}
{% if sections[section] %}
### {{ section }}

{% for category, val in definitions.items() if category in sections[section] %}
{% for text, values in sections[section][category].items() %}
- {{ text }}{% if values %} ({% for value in values %}{{ value }}{% if not loop.last %}, {% endif %}{% endfor %}){% endif %}
{% endfor %}
{% endfor %}
{% endif %}
{% endfor %}

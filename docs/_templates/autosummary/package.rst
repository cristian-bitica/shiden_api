{{ fullname | escape | underline }}

.. automodule:: {{ fullname }}
   :members:
   :undoc-members:
   :show-inheritance:

   {# Module attributes are rendered in the body by ``automodule`` above. A
      summary rubric is deliberately omitted: autosummary reads ``__doc__`` off
      the *value*, so a module-level third-party instance (the FastAPI app, the
      APIKeyHeader scheme) drags that library's whole class docstring in. #}

   {% block classes %}
   {% if classes %}
   .. rubric:: Classes

   .. autosummary::
   {% for item in classes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block functions %}
   {% if functions %}
   .. rubric:: Functions

   .. autosummary::
   {% for item in functions %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

{% block modules %}
{% if modules %}
.. rubric:: Submodules

.. autosummary::
   :toctree:
   :recursive:
{% for item in modules %}
   {{ item }}
{%- endfor %}
{% endif %}
{% endblock %}

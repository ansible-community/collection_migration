"""Helpers for working with Jinja2 templates."""
from pathlib import Path

from jinja2 import Template


def render_template_into(template_path: str, context: dict, target_path: str):
    """Render the template on a given path."""
    template_content = (Path('resources') / template_path).read_text()
    rendered_content = Template(template_content).render(context)
    Path(target_path).write_text(rendered_content)

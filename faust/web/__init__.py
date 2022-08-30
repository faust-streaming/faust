"""Faust Web."""
from faust.types.web import ResourceOptions

from .base import Request, Response, Web
from .blueprints import Blueprint
from .views import View, gives_model, takes_model

__all__ = [
    "Request",
    "Response",
    "ResourceOptions",
    "Web",
    "Blueprint",
    "View",
    "gives_model",
    "takes_model",
]
